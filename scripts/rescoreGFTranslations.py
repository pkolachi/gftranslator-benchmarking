#!/usr/bin/env python

import codecs, copy, itertools, math, multiprocessing, operator, os, os.path, re, string, sys, time;
import pgf;
import gf_utils, translation_pipeline;
import kenlm;

def rescoreHypothesis(languageModel, rankedList):
    for transBlock in rankedList:
	scoredTransBlock = list();
	for hypothesisScores, hypothesis in transBlock:
	    hypothesisScores.insert(0, languageModel.score(hypothesis));
	    scoredTransBlock.append( (hypothesisScores, hypothesis) );
	yield scoredTransBlock;

def rescoreUsingLM(*args):
    lmFile = args[0];
    inputSet = [(transId, hypothesisList) for transId, hypothesisList in gf_utils.readMosesNbestFormat(codecs.open(args[1], 'r') if len(args) > 1 else sys.stdin)];
    sentIdsList = itertools.imap(operator.itemgetter(0), inputSet);
    hypothesisList = itertools.imap(operator.itemgetter(1), inputSet);
    lmModel = kenlm.LanguageModel(lmFile);
    outputPrinter = gf_utils.printMosesNbestFormat;
    for scoredBlock in rescoreHypothesis(lmModel, hypothesisList):
	strTrans = str(outputPrinter(sorted(scoredBlock, key=operator.itemgetter(0), reverse=True), sentIdsList));
	if strTrans:
	    print strTrans;
    return;

if __name__ == '__main__':
    rescoreUsingLM(*sys.argv[1:]);
