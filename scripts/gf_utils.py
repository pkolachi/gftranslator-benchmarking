#!/usr/bin/env python

import codecs, copy, itertools, math, multiprocessing, operator, os, os.path, re, string, sys, time;
import pgf;

#sys.stdin = codecs.getreader('utf-8')(sys.stdin);
#sys.stdout = codecs.getwriter('utf-8')(sys.stdout);
#sys.stderr = codecs.getwriter('utf-8')(sys.stderr);

'''
def convertToGFTokens(conll_sentence):
    tokens = [];
    for entry in conll_sentence:
	# handle numbers;
	# handle named entities;
	# simply lower case the rest;
	tokens.append(token['form'].lower() if token['feats'].find('nertype') == -1 and token['form'] != 'I' else token['form']);
    return " ".join(tokens);

def prepareGFInput(conllfile):
    import conll_utils;
    with codecs.open(conllfile, 'r', 'utf-8') as infile:
	for sentence in conll_utils.sentences_from_conll(infile):
	    yield convertToGFTokens(sentence);
'''

def gf_lexerI(sentence):
    return sentence.rstrip(string.whitespace+string.punctuation);

def gf_lexerChi(sentence):
    sentence = sentence.decode('utf-8');
    tokens, idx, n = [], 0, len(sentence);
    prev = True;
    while idx < n:
	if sentence[idx] in string.whitespace:
	    prev = True;
	    idx += 1;
	    continue;
	if 0 < ord(sentence[idx]) < 128:
	    if sentence[idx] in string.punctuation:
		prev = True;
	    if prev:
		tokens.append( sentence[idx] );
		prev = False;
	    else:
		tokens[-1] = tokens[-1]+sentence[idx];
	else:
	    prev = True;
	    tokens.append( sentence[idx] );
	idx += 1;
    return ' '.join(tokens).encode('utf-8');

def gf_lexer(lang='Eng'):
    if lang[-3:] == 'Eng':
	return gf_lexerI;
    elif lang[-3:] == 'Chi':
	return gf_lexerChi;
    elif lang == 'translator':
	import translation_pipeline_v3;
	return translation_pipeline_v3.pipeline_lexer;
    else:
	return gf_lexerI;

def web_lexer(grammar, lang, sentences):
    tok_sentences = itertools.imap(gf_lexer('translator'), sentences);
    for instance in tok_sentences:
	tokensList = re.split('\s+?', instance);
	token, lowertoken = tokensList[0], tokensList[0].lower();
	count = 0;
	for analysis in grammar.languages[lang].lookupMorpho(lowertoken):
	    count += 1;
	tokensList[0] = lowertoken if count else token;
	for idx, token in enumerate(tokensList):
	    if token.find('-') == -1:
		continue;
	    count = 0;
	    for analysis in grammar.languages[lang].lookupMorpho(token):
		count += 1;
	    if count: 
		continue;
	    token = tokensList[idx].replace('-', '');
	    for analysis in grammar.languages[lang].lookupMorpho(token):
		count += 1;
	    if count:
		tokensList[idx] = token;
		continue;
	    token = tokensList[idx].replace('-', ' ');
	yield ' '.join(tokensList);

def gf_postprocessor(sentence):
    if sentence == None:
	return '';
    if sentence.startswith('* ') or sentence.startswith('% '):
	sentence = sentence[2:];
    sentence = sentence.replace(' &+ ', '');
    sentence = sentence.replace('<+>', ' ');
    return sentence;

def printJohnsonRerankerFormat(gfparsesList, sentid=itertools.count(1)):
    johnsonRepr = [];
    parseHash = {};
    for parse in sorted(gfparsesList, key=operator.itemgetter(0)):
	if not parseHash.has_key(parse[1]):
	    johnsonRepr.append( str(-1*parse[0]) );
	    johnsonRepr.append( str(parse[1]) );
	parseHash.setdefault(parse[1], []).append(parse[0]);
    curid = sentid.next();
    if len(gfparsesList):
	johnsonRepr.insert(0, '%d %d' %(len(parseHash.values()), curid));
    duplicateInstances = len(filter(lambda X: len(parseHash[X]) > 1, parseHash.keys()));
    #if duplicateInstances: print >>sys.stderr, "%d duplicate parses found in K-best parsing" %(duplicateInstances);
    return '\n'.join(johnsonRepr)+'\n';

def readJohnsonRerankerTrees(inputStream):
    endOfParse = False;
    while True:
	sentheader = inputStream.next();
	if sentheader == '':
	    break;
	parsescount, sentidx = map(int, sentheader.strip().split());
	parsesBlock = [];
	for i in xrange(parsescount):
	    parseprob = inputStream.next();
	    if parseprob.strip() == '':
		endOfParse = True;
		break;
	    parse = inputStream.next();
	    parsesBlock.append( (float(parseprob.strip()), pgf.readExpr(parse.strip())) );
	yield sentidx, parsesBlock;
	if not endOfParse:
	    _ = inputStream.next();
	endOfParse = False;

def printMosesNbestFormat(hypothesisList, sentid=itertools.count(1)):
    mosesRepr = [];
    sid = sentid.next();
    for hypScores, hypStr in hypothesisList:
	mosesRepr.append("%d ||| %s ||| NULL ||| %s" %(sid, hypStr, ' '.join(['%.6f'%score for score in hypScores])));
    return '\n'.join(mosesRepr);

def readMosesNbestFormat(inputStream):
    transBlock = [];
    currentHypothesisId = 0;
    while True:
	line = inputStream.next();
	if line == '':
	    break;
	fields = line.strip().split('|||');
	if str(fields[0].strip()) != str(currentHypothesisId):
	    yield currentHypothesisId, transBlock;
	    transBlock = [];
	    currentHypothesisId = int(fields[0]);
	transBlock.append( (map(float, tuple([val.strip() for val in fields[3].split()])), fields[1].strip()) );

def getKTranslations(grammar, tgtlanguage, abstractParsesList):
    generator = grammar.languages[tgtlanguage].linearize;
    for parsesBlock in abstractParsesList:
	kBestTrans = [];
	for parseprob, parse in parsesBlock:
	    #print str(parse);
	    kBestTrans.append( ((parseprob,), gf_postprocessor( generator(parse) )) );
	yield kBestTrans;

def getKBestParses(grammar, language, K, callbacks=[], serializable=False, sentid=itertools.count(1)):
    parser = grammar.languages[language].parse;
    def worker(sentence):
	sentence = sentence.strip();
	curid = sentid.next();
	tstart = time.time();
	kBestParses = [];
	parseScores = {};
	try:
	    for parseidx, parse in enumerate( parser(sentence, heuristics=0, callbacks=callbacks) ):
		parseScores[parse[0]] = True;
		kBestParses.append( (parse[0], str(parse[1]) if serializable else parse[1]) );
		if parseidx == K-1: break;
		#if len(parseScores) >= K: break;
	    tend = time.time();
	    print >>sys.stderr, '%d\t%.4f' %(curid, tend-tstart);
	    return tend-tstart, kBestParses;
	except pgf.ParseError, err:
	    tend = time.time();
	    print >>sys.stderr, '%d\t%.4f\t%s' %(curid, tend-tstart, err);
	    return tend-tstart, kBestParses;
	except UnicodeEncodeError, err:
	    tend = time.time();
	    print >>sys.stderr, '%d\t%.4f\t%s' %(curid, tend-tstart, err);
	    return tend-tstart, kBestParses;
    return worker;

def getMultiParses(grammar, language, bagSize=0.001, callbacks=[], serializable=False, sentid=itertools.count(1)):
    parser = grammar.languages[language].parse;
    logRange = math.log(bagSize);
    logTol   = 0;
    def worker(sentence):
	sentence = sentence.strip();
	curid = sentid.next();
	tstart = time.time();
	kBestParses, maxprob, firstprob = [], sys.maxint, sys.maxint;
	try:
	    for parseidx, parse in enumerate( parser(sentence, heuristics=0, callbacks=callbacks) ):
		if firstprob == sys.maxint:
		    firstprob = parse[0];
		maxprob = min(maxprob, parse[0]);
		if parse[0] > firstprob-logRange+logTol: 
		    break;
		kBestParses.append( (parse[0], str(parse[1]) if serializable else parse[1]) );
	    tend = time.time();
	    print >>sys.stderr, '%d\t%.4f' %(curid, tend-tstart);
	    #yield sorted(filter(lambda X: X[0]<=(maxprob-logRange), kBestParses), key=operator.itemgetter(0));
	    return tend-tstart, filter(lambda X: X[0]<=(maxprob-logRange), kBestParses);
	except pgf.ParseError, err:
	    tend = time.time();
	    print >>sys.stderr, '%d\t%.4f\t%s' %(curid, tend-tstart, err);
	    #yield sorted(filter(lambda X: X[0]<=(maxprob-logRange), kBestParses), key=operator.itemgetter(0));
	    return tend-tstart, filter(lambda X: X[0]<=(maxprob-logRange), kBestParses);
	except UnicodeEncodeError, err:
	    tend = time.time();
	    print >>sys.stderr, '%d\t%.4f\t%s' %(curid, tend-tstart, err);
	    #yield sorted(filter(lambda X: X[0]<=(maxprob-logRange), kBestParses), key=operator.itemgetter(0));
	    return tend-tstart, filter(lambda X: X[0]<=(maxprob-logRange), kBestParses);
    return worker;

def pgf_parse(*args):
    grammarfile, start_cat, lang = args[0], args[1], args[2];
    #inputSet = itertools.imap(gf_lexer('translator'), codecs.open(args[3], 'r', 'utf-8') if len(args) > 3 else sys.stdin);
    grammar = pgf.readPGF(grammarfile);
    
    import translation_pipeline_v3;
    callbacks = [('PN', translation_pipeline_v3.parseNames(grammar, lang)), ('Symb', translation_pipeline_v3.parseUnknown(grammar, lang))];

    inputSet = web_lexer(grammar, lang, codecs.open(args[3], 'r') if len(args) > 3 else sys.stdin);
    outputPrinter = lambda X: "%f\t%s" %(X[0], str(X[1])); #operator.itemgetter(1);
    parser = getKBestParses(grammar, lang, 1, callbacks);

    sentidx = 0;
    for time, parsesBlock in itertools.imap(parser, inputSet):
	sentidx += 1;
	print "%d\t%f\t%s" %(sentidx, time, str(outputPrinter(parsesBlock[0])) if len(parsesBlock) else '');
    return;

def pgf_kparse(*args):
    grammarfile, start_cat, lang, K = args[0], args[1], args[2], int(args[3]);
    #inputSet = itertools.imap(gf_lexer('translator'), codecs.open(args[4], 'r') if len(args) > 4 else sys.stdin);
    grammar = pgf.readPGF(grammarfile);
    
    import translation_pipeline_v3;
    callbacks = [('PN', translation_pipeline_v3.parseNames(grammar, lang)), ('Symb', translation_pipeline_v3.parseUnknown(grammar, lang))];

    inputSet = web_lexer(grammar, lang, codecs.open(args[4], 'r') if len(args) > 4 else sys.stdin);
    outputPrinter = printJohnsonRerankerFormat;
    parser = getKBestParses(grammar, lang, K, callbacks=callbacks);

    for time, parsesBlock in itertools.imap(parser, inputSet):
	strParses = str(outputPrinter(parsesBlock));
	if not (strParses == '\n'):
	    print strParses;
    return;

def pgf_multiparse(*args):
    grammarfile, start_cat, lang, beam = args[0], args[1], args[2], float(args[3]);
    #inputSet = itertools.imap(gf_lexer('translator'), codecs.open(args[4], 'r') if len(args) > 4 else sys.stdin);
    grammar = pgf.readPGF(grammarfile);
    
    import translation_pipeline_v3;
    callbacks = [('PN', translation_pipeline_v3.parseNames(grammar, lang)), ('Symb', translation_pipeline_v3.parseUnknown(grammar, lang))];

    inputSet = web_lexer(grammar, lang, codecs.open(args[4], 'r') if len(args) > 4 else sys.stdin);
    outputPrinter = printJohnsonRerankerFormat;
    parser = getMultiParses(grammar, lang, beam, callbacks=callbacks);

    for time, parsesBlock in itertools.imap(parser, inputSet):
	strParses = str(outputPrinter(parsesBlock));
	if not (strParses == '\n'):
	    print strParses;
    return;

def pgf_translate(*args):
    grammarfile, start_cat, srclang, tgtlang = args[0], args[1], args[2], args[3];
    inputSet = itertools.imap(gf_lexer('translator'), codecs.open(args[4], 'r', 'utf-8') if len(args) > 4 else sys.stdin);
    grammar = pgf.readPGF(grammarfile);
    outputPrinter = operator.itemgetter(1);
    for transBlock in getKTranslations(grammar, tgtlang, getKBestParses(grammar, srclang, inputSet, K=1)):
	print str(outputPrinter(transBlock[0])) if len(transBlock) else '';
    return;

def pgf_ktranslate(*args):
    grammarfile, start_cat, srclang, tgtlang, K = args[0], args[1], args[2], args[3], int(args[4]);
    inputSet = itertools.imap(gf_lexer('translator'), codecs.open(args[5], 'r', 'utf-8') if len(args) > 5 else sys.stdin);
    grammar = pgf.readPGF(grammarfile);
    outputPrinter = printMosesNbestFormat;
    for transBlock in getKTranslations(grammar, tgtlang, getKBestParses(grammar, srclang, inputSet, K)):
	strTrans = str(outputPrinter(transBlock));
	if strTrans:
	    print strTrans;
    return;

def pgf_linearize(*args):
    grammarfile, tgtlang = args[0], args[1];
    inputSet = itertools.imap(pgf.readExpr, codecs.open(args[2], 'r') if len(args) > 2 else sys.stdin);
    grammar    = pgf.readPGF(grammarfile);
    linearizer = grammar.languages[tgtlang].linearize;
    for linString in itertools.imap(linearizer, inputSet):
	print gf_postprocessor( linString.decode('utf-8') );
    return;

def pgf_klinearize(*args):
    grammarfile, tgtlang = args[0], args[1];
    inputSet = [(sentid, parsesBlock) for sentid, parsesBlock in readJohnsonRerankerTrees( codecs.open(args[2], 'r', 'utf-8') if len(args) > 2 else sys.stdin )];
    sentIdsList  = itertools.imap(operator.itemgetter(0), inputSet);
    parsesBlocks = map(operator.itemgetter(1), inputSet);
    grammar = pgf.readPGF(grammarfile);
    outputPrinter = printMosesNbestFormat;
    for transBlock in getKTranslations(grammar, tgtlang, parsesBlocks):
	strTrans = str(outputPrinter(transBlock, sentIdsList));
	if strTrans:
	    print strTrans;
    return;

if __name__ == '__main__':
    pgf_parse(*sys.argv[1:]);
    #pgf_kparse(*sys.argv[1:]);
    #pgf_multiparse(*sys.argv[1:]);
    #pgf_linearize(*sys.argv[1:]);
    #pgf_klinearize(*sys.argv[1:]);
    #pgf_translate(*sys.argv[1:]);
    #pgf_ktranslate(*sys.argv[1:]);
    #sys.exit(0);
