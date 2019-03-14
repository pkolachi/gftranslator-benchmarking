#!/usr/bin/env python3

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

def bilinks2inter(bidict, uttlngmap, null_def='-', heuristic='intersect') : 
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
  DEF_NULL = '-' ;
  interlnks = [] ;
  lnglst = set(uttlngmap.values()) ;
  if heuristic == 'intersect' :
    # only keep those entries which are attested by original ids
    for lnk in lcllinks :
      lnkgrp = [] ; 
      for lng in lnglst : 
        pts = tuple(pt for pt in lnk if uttlngmap[pt] == lng) ;
        if not len(pts) :
          pts = (null_def, ) ; 
        lnkgrp.append(pts) ; 
      for grp in it.product(*lnkgrp) :
        pts = [pt for pt in grp if pt != null_def] ; 
        isintersect = all(\
            True if pair[1] in bidict[pair[0]] else False \
            for pair in it.combinations(pts, 2)) ; 
        if isintersect : 
          interlnks.append(grp) ; 

  return interlnks ; 


def main() :
  global corpus_prefix, links_prefix, tags_prefix ; 
  lcfpth = sys.argv[1] ;   # file path of filtered language codes 
  fillcs = [l.strip() for l in io.open(lcfpth)] ; 
  datadr = sys.argv[2] ;   # directory that points to tatoeba data
  outpdr = sys.argv[3] ;   # directory to write the output files 
  
  # assumes following files are available in the directory
  # 1. datadr/sentences_detailed.csv
  # 2. datadr/links.csv
  # 3. datadr/tags.csv
  datafls = os.listdir(datadr) ;
  if not ( corpus_prefix in datafls and \
           links_prefix  in datafls and \
           tags_prefix   in datafls ) :
    print("Relevant files could be missing from the \
           project dir {0}".format(datadr), file=sys.stderr) ; 
    sys.exit(1) ; 
  # obtain full paths for relevant files 
  crpfpth = os.path.join(datadr, corpus_prefix) ; 
  lnkfpth = os.path.join(datadr, links_prefix)  ; 
  tgsfpth = os.path.join(datadr, tags_prefix)   ; 

  # read full corpus file but only keep utterances in the filtered languages
  sls = (l.strip() for l in io.open(crpfpth, encoding='utf-8')) ;
  tls = (X.split('\t') for X in sls) ; 
  fls = ((X[0], X[1], X[2]) for X in tls if X[1] in fillcs) ; 
  # load the corpus to memory for the filtered languages
  corpus = defaultdict(dict) ; # primary key is iso-3, secondary key is utt.id
  lngsid = {} ;                # utt. id => iso-3 code
  uttc   = 0 ; 
  for X in fls : 
    corpus[X[1]][X[0]] = X[2] ;
    lngsid[X[0]]       = X[1] ;
    uttc += 1 ;
  print("Loaded {0} utterances from {1} languages".format(uttc, len(fillcs)), \
         file=sys.stderr) ; 
  
  # read utt. alignment links
  sls = (l.strip() for l in io.open(lnkfpth, encoding='utf-8')) ; 
  tls = map(lambda X: tuple(X.split()), sls) ; 
  fls = (X for X in tls if X[0] in lngsid and X[1] in lngsid) ; 
  # load the links into bilingual table with lists
  bilnks = defaultdict(set) ;
  mlnks  = defaultdict(int)  ;  # counter abt how many are monolingual alignments
  lnkc   = 0 ; 
  for X in fls :
    # force this item to be symmetric
    bilnks[X[0]].add(X[1]) ;
    bilnks[X[1]].add(X[0]) ;
    if lngsid[X[0]] == lngsid[X[1]] :
      mlnks[lngsid[X[0]]] += 1 ; 
    lnkc += 1 ;
  print("Loaded {0} alignments".format(lnkc), file=sys.stderr) ; 
  mlnkstr = '\n'.join('{0}\t{1}'.format(lng,mlnks[lng]) for lng in fillcs) ;
  print(mlnkstr, file=sys.stderr) ; 

  # this is the actual function that matters
  # different ways to convert bilingual links used in parallel corpora
  # to interlingual links
  interlnks = bilinks2inter(bilnks, lngsid, heuristic='intersect') ;  
  
  # write interlingual link information
  lnkofpth = os.path.join(outpdr, 'interlingua_links.csv') ;
  with io.open(lnkofpth, 'w', encoding='utf-8') as outf :
    for plnk in interlingual_links(interlnks, fillcs, lngsid) : 
      for uttid in plnk :
        tap[uttid] = True ; 
      print(','.join(plnk), file=outf) ; 
  # write sentences in each language to seperate files 
  for lng in fillcs :
    outpth = os.path.join(outpdr, 'sentences_split.{0}.csv'.format(lng)) ;
    snts   = ('\t'.join(ut) for ut in sorted(corpus[lng].items()) if ut[0] in tap) ;
    with io.open(outpth, 'w', encoding='utf-8') as outf :
      for ut in snts :
        print(ut, file=outf) ;

  return True ; 

if __name__ == '__main__' :
  main() ; 
