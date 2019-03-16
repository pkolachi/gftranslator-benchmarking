#!/usr/bin/env python3

from __future__ import print_function ; # in case I accidentally run using Py2

import argparse ; 
import io  ; 
import itertools as it ; 
import os  ; 
import os.path  ; 
import sys ; 
from collections import defaultdict ; 
from functools   import partial ; 
from operator    import itemgetter ; 
if sys.version_info >= (3,0) : 
  print = partial(print, flush=True) ; 


"""
This script takes the corpus from the tatoeba project and creates a multi-way
corpus for selected languages.
After I started writing the script, I found an implementation on github 
https://github.com/jonsafari/multiway-corpus
This script differs from the above implementation because:
  a) The previous implementation finds an alignment by taking the smallest 
  language corpus and finding one translation in each language aligned to 
  a sentence in the smallest language.
  b) Instead, in bilinks2inter[1] I use an approach from clustering:
    i)  create small connected components in the sparse graph
    ii) extract paths within each component such that each node in the path 
    belongs to a different language 
  This makes it possible to extract multiway translations even if 
  translations are not available in all languages. 
  The problem with this approach is there are too many paths even when tested
  for a small #languages (I haven't managed to run it for 6 languages, 3 seems
  to work alright)

The resulting interlinks.tsv gives a score for each multiway alignment- which
for n languages, is upper-bounded by C(n,2). The script also filters alignments
using a lower-bound of 2. 

TODO: I am unable to run the script with Python 2- only tested it with Python 3
Hence one needs to check if the implementation is sound and doesn't yield
strange results due to Python 3.

"""

corpus_prefix = 'sentences.csv' # 'sentences_detailed.csv' ; 
links_prefix  = 'links.csv' ; 
tags_prefix   = 'tags.csv' ; 

def readcorpus(crpfpth, langs) : 
  # read full corpus file but only keep utterances in the filtered languages
  sls = (l.strip() for l in io.open(crpfpth, encoding='utf-8')) ; 
  tls = (X.split('\t') for X in sls) ; 
  fls = ((X[0], X[1], X[2]) for X in tls if len(X) > 2 and X[1] in langs) ; 
  # load the corpus to memory for the filtered languages
  corpus = defaultdict(dict) ; # primary key is iso-3, secondary key is utt.id
  lngsid = {} ;                # utt. id => iso-3 code
  uttc   = 0 ; 
  for X in fls : 
    corpus[X[1]][X[0]] = X[2] ; 
    lngsid[X[0]]       = X[1] ; 
    uttc += 1 ; 
  print("Loaded {0} utterances from {1} languages- {2}".format(uttc,
         len(langs),
         ', '.join(langs)),
         file=sys.stderr) ; 
  for lng in langs : 
    lngc = sum(1 for _ in lngsid if lngsid[_] == lng) ; 
    print("Loaded {0} utterances for {1}".format(lngc, lng), file=sys.stderr) ; 
  return (corpus, lngsid) ; 

def readbilinks(lnkfpth, uttlngmap) : 
  # read utt. alignment links
  sls = (l.strip() for l in io.open(lnkfpth, encoding='utf-8')) ; 
  tls = (X.split() for X in sls) ; 
  fls = (X for X in tls if len(X) == 2 and 
                           X[0] in uttlngmap and X[1] in uttlngmap) ; 
  # load the links into bilingual table with lists
  bilnks = defaultdict(set) ; 
  mlnks  = defaultdict(int)  ;  # counter of monolingual alignments
  langs  = set(uttlngmap.values()) ; 
  lnkc   = 0 ; 
  for X in fls :
    # force this item to be symmetric
    bilnks[X[0]].add(X[1]) ; 
    bilnks[X[1]].add(X[0]) ; 
    if uttlngmap[X[0]] == uttlngmap[X[1]] :
      mlnks[uttlngmap[X[0]]] += 1 ; 
    lnkc += 1 ; 
  print("Loaded {0} alignments".format(lnkc), file=sys.stderr) ; 
  mlnkstr = '\n'.join('{0}\t{1}'.format(lng,mlnks[lng]) for lng in langs) ; 
  print(mlnkstr, file=sys.stderr) ; 
  return bilnks ; 

def writeinterlnks(interlnks, corpus, lnkfpth, corpus_prefix) :
  tap = {} ;    # only write those utts. that have atleast one alignment
  # write interlingual link information
  print("{0} links found in the corpus".format(len(interlnks)), 
      file=sys.stderr)  ; 
  langs = sorted(corpus.keys()) ; 
  with io.open(lnkfpth, 'w', encoding='utf-8') as outf :
    print(','.join(langs), file=outf) ; 
    for plnk in sorted(interlnks, 
                      key=lambda X: (len([1 for _ in X if _]), interlnks[X]), 
                      reverse=True) :
      for uttid in plnk :
        tap[uttid] = True ; 
      plnkstr = ','.join(x if x else '-' for x in plnk) ; 
      print("{0}\t{1}".format(plnkstr, interlnks[plnk]), file=outf) ; 
  # write sentences in each language to seperate files 
  for lng in langs :
    outfpth = '{0}.{1}.csv'.format(corpus_prefix, lng) ; 
    snts = ('\t'.join(ut) for ut in sorted(corpus[lng].items()) if ut[0] in tap) ; 
    with io.open(outfpth, 'w') as outf :
      for ut in snts :
        print(ut, file=outf) ; 
  return ; 

def cmdline() :
  argparser = argparse.ArgumentParser(prog=sys.argv[0], description='Prepare multilingual corpus from Tatoeba parallel translations') ; 
  # languages to be filtered / selected
  argparser.add_argument('-l', '--langs',  dest='langs',   nargs='*', default=[], help='iso-3 code for selected languages') ; 
  argparser.add_argument('-c', '--corpus', dest='crpfpth', required=True,  help='corpus tsv file from the project') ; 
  argparser.add_argument('-b', '--links',  dest='lnkfpth', required=True,  help='bilingual links from the project') ; 
  argparser.add_argument('-t', '--tags',   dest='tagfpth', required=False, help='tags information from the project') ; 
  argparser.add_argument('-o', '--outdir', dest='outdir',  default='',     help='output directory') ; 
  argparser.add_argument('--all', dest='alllangs', default=False, help='only include those links with utterances in all languages') ; 
  return argparser ; 


def bilinks2inter(bidict, uttlngmap) : 
  # bidict is a dictionary with bidict[a] == [b] if (a,b) is an possible link
  # interdict is a dictionary with inter[m] == n if (m,n) is a link in the transitive closure of bidict

  deg = defaultdict(int) ; 
  for X in bidict :
    deg[X] += len(bidict[X])  ; 
    for y in bidict[X] : 
      deg[y] += 1 ; 

  # utt ids to be processed in descending order of degree 
  sdeg = iter(sorted(deg, key=itemgetter(1), reverse=True)) ; 
  proc_ids = {} ;    # utt ids that have already been grouped
  lcllinks = [] ; 
  for pt in sdeg : 
    if pt in proc_ids :
      continue ; 
    lcl = [] ; 
    lcl.append(pt) ; 
    cur = 0 ; 
    while cur < len(lcl) :
      tpt = lcl[cur] ; 
      # all points aligned to tpt not yet included in local group
      buf = sorted((npt for npt in bidict[tpt] if npt not in lcl), 
                   key=lambda x: deg[x], reverse=True) ; 
      lcl.extend(buf) ; 
      cur += 1 ; 
    if cur > 1 :   # node is not isolated and has atleast one translation
      lcllinks.append( tuple(lcl) ) ; 
    # add all points in lcl to proc_ids
    for cpt in lcl : 
      proc_ids[cpt] = True ; 
  print("Found {0} local groups from the parallel alignments".format(len(lcllinks)), file=sys.stderr) ; 

  interlnks = defaultdict(float) ; 
  langs     = set(uttlngmap.values()) ; 
  for idx,lnk in enumerate(lcllinks, start=1) :
    if not (idx % 10000) :
      print("Processed {0} local groups to create {1} entries".format(idx, len(interlnks)), file=sys.stderr) ; 
    # make partitions s.t each partition maps to a different language 
    lnktbl = defaultdict(list) ; 
    for pt in lnk : 
      lnktbl[uttlngmap[pt]].append(pt) ; 
    lnkgrp = [tuple(lnktbl[lng]) if lnktbl[lng] else (None,) for lng in langs] ; 

    # use an incremental way to construct combinations of localgrps
    # brute force approach of it.product ++ it.combinations is too slow even for 3 languages
    intlnk  = [([pt],0) for pt in lnkgrp[0]] ; 
    for mgrp in lnkgrp[1:] : 
      bufintlnk  = [] ;  # a buffer to store valid links 
      for pool,spool in intlnk :
        for itm in mgrp :
          snew = spool + sum(1 for pt in pool if pt in bidict[itm]) ; 
          bufintlnk.append((pool + [itm], snew)) ; 
      intlnk = bufintlnk ; 
    """
    intlnk  = [[pt] for pt in lnkgrp[0]] ; 
    sintlnk = [0    for pt in lnkgrp[0]] ; 
    for mgrp in lnkgrp[1:] : 
      bufintlnk  = [] ;  # a buffer to store valid links 
      sbufintlnk = [] ; 
      for pool,spool in zip(intlnk, sintlnk) :
        for itm in mgrp :
          snew = spool + sum(1 for pt in pool if pt in bidict[itm]) ; 
          bufintlnk.append(pool + [itm]) ; 
          sbufintlnk.append(snew) ; 
      newintlnk = sorted(zip(bufintlnk, sbufintlnk), key=itemgetter(1)) ;
      # since we iterate multiple times over this list, we need to build the full lists each time 
      intlnk  = map(itemgetter(0), newintlnk) ; 
      sintlnk = map(itemgetter(1), newintlnk) ; 
    intlnk = zip(intlnk, sintlnk) ; 
    """
    for lnk,slnk in intlnk :
      if slnk >= 1 :
        interlnks[tuple(lnk)] = slnk ; 

  return interlnks ; 


def main() :
  runenv = cmdline().parse_args(sys.argv[1:]) ; 

  crpfpth = runenv.crpfpth ; 
  lnkfpth = runenv.lnkfpth ; 
  if hasattr(runenv, 'tagfpth') :
    tagfpth = runenv.tagfpth ; 
  else :
    tagfpth = None ; 
  outpdr = runenv.outdir ; 

  fillcs = runenv.langs if runenv.langs else [] ; 
  (corpus, lngsid) = readcorpus(crpfpth, fillcs) ; 
  bilnks = readbilinks(lnkfpth, lngsid) ; 
  # this is the actual function that matters
  # different ways to convert bilingual links used in parallel corpora
  # to interlingual links
  interlnks = bilinks2inter(bilnks, lngsid) ; 
  if runenv.alllangs :
    interlnks = dict((lnk,interlnks[lnk]) for lnk in interlnks if None not in lnk) ; 

  if not os.path.isdir(runenv.outdir) : 
    os.makedirs(runenv.outdir) ; 
  lnkfpth = os.path.join(runenv.outdir, 'interlinks.csv') ; 
  oprefix = os.path.join(runenv.outdir, 'sentences') ; 
  writeinterlnks(interlnks, corpus, lnkfpth, oprefix) ; 

  return True ; 

if __name__ == '__main__' :
  main() ; 

