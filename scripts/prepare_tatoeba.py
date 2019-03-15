#!/usr/bin/env python3

import argparse ; 
import io  ;
import itertools as it ; 
import os  ;
import os.path  ;
import sys ; 
from collections import defaultdict ; 
from operator    import itemgetter ; 


corpus_prefix = 'sentences_detailed.csv' ; 
links_prefix  = 'links.csv' ;
tags_prefix   = 'tags.csv' ; 

def readcorpus(crpfpth, langs) : 
  # read full corpus file but only keep utterances in the filtered languages
  sls = (l.strip() for l in io.open(crpfpth, encoding='utf-8')) ;
  tls = (X.split('\t') for X in sls) ; 
  fls = ((X[0], X[1], X[2]) for X in tls if X[1] in langs) ; 
  # load the corpus to memory for the filtered languages
  corpus = defaultdict(dict) ; # primary key is iso-3, secondary key is utt.id
  lngsid = {} ;                # utt. id => iso-3 code
  uttc   = 0 ; 
  for X in fls : 
    corpus[X[1]][X[0]] = X[2] ;
    lngsid[X[0]]       = X[1] ;
    uttc += 1 ;
  print("Loaded {0} utterances from {1} languages- {2}".format(uttc, \
         len(langs), \
         ','.join(langs)),
         file=sys.stderr) ; 
  return (corpus, lngsid) ; 

def readbilinks(lnkfpth, uttlngmap) : 
  # read utt. alignment links
  sls = (l.strip() for l in io.open(lnkfpth, encoding='utf-8')) ; 
  tls = (X.split() for X in sls) ; 
  fls = (X for X in tls if X[0] in uttlngmap and X[1] in uttlngmap) ; 
  # load the links into bilingual table with lists
  bilnks = defaultdict(set) ;
  mlnks  = defaultdict(int)  ;  # counter abt how many are monolingual alignments
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
  with io.open(lnkfpth, 'w', encoding='utf-8') as outf :
    for plnk in sorted(interlnks, key=lambda X: len([_ for _ in X if _]), reverse=True) :
      for uttid in plnk :
        tap[uttid] = True ;
      plnkstr = ','.join(x if x else '-' for x in plnk) ; 
      print(plnkstr, file=outf) ; 
  # write sentences in each language to seperate files 
  langs = corpus.keys() ; 
  for lng in langs :
    outfpth = '{0}.{1}.csv'.format(corpus_prefix, lng) ;
    snts    = ('\t'.join(ut) for ut in sorted(corpus[lng].items()) if ut[0] in tap) ;
    with io.open(outfpth, 'w', encoding='utf-8') as outf :
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
  return argparser ; 


def bilinks2inter(bidict, uttlngmap, heuristic='intersect') : 
  possible_heuristics = ['intersect', 'union'] ;  
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
      for npt in bidict[tpt] : 
        if   npt not in lcl  and  heuristic == 'grow' :
          lcl.append(npt) ;
        elif npt not in lcl  and  uttlngmap[tpt] != uttlngmap[npt] :
          lcl.append(npt) ; 
      cur += 1 ;

    lcllinks.append( tuple(sorted(lcl)) ) ; 
    # add all points in lcl to proc_ids
    for cpt in lcl : 
      proc_ids[cpt] = True ;
  
  # apply heuristics to convert local groups to interlingual tables 
  interlnks = [] ;
  lnglst = set(uttlngmap.values()) ;
  if heuristic == 'intersect' :
    # only keep those entries which are attested by original ids
    for lnk in lcllinks :
      lnkgrp = [] ; 
      for lng in lnglst : 
        pts = tuple(pt for pt in lnk if uttlngmap[pt] == lng) ;
        if not len(pts) :
          pts = (None, ) ; 
        lnkgrp.append(pts) ; 
      for grp in it.product(*lnkgrp) :
        pts = [pt for pt in grp if pt] ;
        if len(pts) == 1 : 
          continue ; 
        isintersect = all(\
            True if pair[1] in bidict[pair[0]] else False \
            for pair in it.combinations(pts, 2)) ; 
        if isintersect : 
          interlnks.append(grp) ; 

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
  interlnks = bilinks2inter(bilnks, lngsid, heuristic='intersect') ;  

  if not os.path.isdir(runenv.outdir) : 
    os.makedirs(runenv.outdir) ; 
  lnkfpth = os.path.join(runenv.outdir, 'interlinks.csv') ; 
  oprefix = os.path.join(runenv.outdir, 'sentences') ; 
  writeinterlnks(interlnks, corpus, lnkfpth, oprefix) ; 

  return True ; 

if __name__ == '__main__' :
  main() ; 

