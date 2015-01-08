#!/usr/bin/env python

import argparse, codecs, copy, itertools, logging, math, operator, os, os.path, re, string, sys, time;
#import lxml.etree as etree;
import xml.etree.ElementTree as etree;
import pgf;
import gf_utils;

# http://snipplr.com/view/25657/indent-xml-using-elementtree/
def indentXMLNodes(elem, level=0):
    i = "\n" + level*"  "
    if len(elem):
	if not elem.text or not elem.text.strip():
	    elem.text = i + "  "
	if not elem.tail or not elem.tail.strip():
	    elem.tail = i
	for elem in elem:
	    indentXMLNodes(elem, level+1)
	if not elem.tail or not elem.tail.strip():
	    elem.tail = i
    else:
	if level and (not elem.tail or not elem.tail.strip()):
	    elem.tail = i

GRAMMAR = '';
SRCLANG = '';
TGTLANGS = '';

def readTranslationPipelineOptions(propsfile, default_namespace):
    with codecs.open(propsfile, 'r', 'utf-8') as infile:
	for line in infile:
	    if not line.strip():
		continue;
	    key, value = line.strip().split('=', 1);
	    key, value = key.strip(), value.strip();
	    if   key == 'srclang':  default_namespace.srclang = value;
	    elif key == 'tgtlangs': default_namespace.tgtlangs = [val.strip() for val in ','.split(value)];
	    elif key == 'input':    default_namespace.input = value;
	    elif key == 'format':   default_namespace.format = value;
	    elif key == 'exp_directory': default_namespace.exp_directory = value;
	    else:
		#print >>sys.stderr, "Unknown option-%s found in props file. Ignoring and proceeding." %(key);
		logging.warning("Unknown option-%s found in props file. Ignoring and proceeding." %(key));
		continue;
    return default_namespace;

def sgmReader(sgmDoc):
    root = sgmDoc.getroot();
    for element in root.iter():
	if element.text is not None and element.text.strip():
	    yield element.text.strip().encode('utf-8');

def addToSgm(sgmDoc, strItem):
    for node in sgmDoc.findall('.//seg'):
	if not node.text.strip():
	    strItem = strItem.decode('utf-8');
	    node.text = ' %s ' %(strItem if strItem.strip() else 'EMPTY');
	    return;
    logging.error("No more nodes available for adding content");
    return;

def sgmWriter(sgmDoc):
    indentXMLNodes( sgmDoc.getroot() );
    return etree.tostring(sgmDoc.getroot(), encoding='utf-8', method='xml');

def getXMLSkeleton(sgmDoc, tgtlang):
    skeletonDoc = copy.deepcopy(sgmDoc);
    root = skeletonDoc.getroot();
    root.tag = 'tstset';
    root.attrib['trlang'] = tgtlang[-3:];
    root.find('doc').attrib['sysid'] = tgtlang[:-3];
    for node in root.findall('.//seg'):
	node.text = '';
    return skeletonDoc;

def pipeline_lexer(sentence):
    tokens = sentence.strip().split();
    #tokens = filter(None, re.split('(\W+)', sentence.strip()));
    n = len(tokens);
    idx = len(tokens)-1;
    while idx >= 0:
	if tokens[idx] in ".?!":
	    idx -= 1;
	else:
	    break;
    return ' '.join(tokens[:idx+1]);

def clean_gfstrings(sentence):
    absFuncName = re.compile('\[[^]]+?\]');
    untranslatedEntries = {};
    for entry in re.findall(absFuncName, sentence):
	untranslatedEntries[entry] = untranslatedEntries.setdefault(entry, 0)+1;
    for entry in untranslatedEntries:
	while untranslatedEntries[entry] > 1:
	    sentence = sentence.replace(entry, '', 1);
	    untranslatedEntries[entry] -= 1;
	sentence = sentence.replace(entry, ' '.join(entry[1:-1].split('_')[:-1]) if entry.find('_') != -1 else '');
    return ' '.join( sentence.split() );

def parseNames(lin_idx, sentence, start):
    global GRAMMAR, SRCLANG;
    moving_start, end, eot = start, len(sentence), True;
    if moving_start < end and (not sentence[moving_start].isupper()):
	return None;
    while moving_start < end:
	if sentence[moving_start] in string.whitespace:
	    eot = True;
	elif eot and sentence[moving_start].isupper()::
	    eot = False;
	    continue;
	elif eot and (not sentence[moving_start].isupper()):
	    end = moving_start-1;
	    break;
	moving_start += 1;
    possible_name = sentence[start:end].strip();
    if possible_name:
	if SRCLANG.endswith('Eng') and (possible_name == "I" or possible_name == "I'm"):
	    return None;
	elif SRCLANG.endswith('Eng') and possible_name.endswith("'s"):
	    end_idx = possible_name.rfind("'s");
	    if end_idx != -1: 
		end = end_idx;
		possible_name = possible_name[:end].strip();
	expr, prob = None, None;
	for analysis in GRAMMAR.languages[SRCLANG].lookupMorpho(possible_name):
	    category = GRAMMAR.functionType(analysis[0]).cat;
	    if prob < analysis[-1]:
		if category == "PN":
		    expr, prob = pgf.Expr(analysis[0], []), analysis[-1];
		elif category == "Weekday":
		    expr, prob = pgf.Expr("weekdayPN", [pgf.Expr(analysis[0], [])]), analysis[-1];
		elif category == "Month":
		    expr, prob = pgf.Expr("monthPN", [pgf.Expr(analysis[0], [])]), analysis[-1];
		elif category == "Language":
		    return None;
	# generic named entity
	if expr == None:
	    expr = pgf.Expr(possible_name);
	    expr = pgf.Expr("MkSymb", [expr]);
	    expr = pgf.Expr("SymbPN", [expr]);
	return (expr, 0, end);
    return None;

def parseUnknown(lin_idx, sentence, start):
    global GRAMMAR, SRCLANG;
    moving_start, end, eot = start, len(sentence), True;
    if moving_start < end and not sentence[moving_start].isupper():
	while moving_start < end:
	    if sentence[moving_start] in string.whitespace:
		end = moving_start;
		break;
	    moving_start += 1;
	unknown_word = sentence[start:end].strip();
	if unknown_word:
	    count = 0;
	    for analysis in GRAMMAR.languages[SRCLANG].lookupMorpho(unknown_word):
		count += 1;
	    if not count:
		expr = pgf.Expr("MkSymb", [pgf.Expr(unknown_word)]);
		return (expr, 0, end);
    return None;

def parseTester(lin_idx, sentence, start):
    if start < len(sentence):
	return (pgf.Expr(sentence[start]), 0, start+1);
    return None;

def translateWord(word, tgtlanguage):
    global GRAMMAR, SRCLANG, TGTLANGS;
    lowerword = word.lower();
    try:
	partialExprList = GRAMMAR.languages[SRCLANG].parse(word, cat='Chunk');
	for expr in partialExprList:
	    trans = grammar.languages[tgtlanguage].linearize(expr[1]);
	    if not trans:
		print expr[1], tgtlanguage;
	    return gf_utils.gf_postprocessor( trans if trans else ' ' );
    except pgf.ParseError:
	morphAnalysis = GRAMMAR.languages[SRCLANG].lookupMorpho(word) + GRAMMAR.languages[SRCLANG].lookupMorpho(lowerword);
	for morph in morphAnalysis:
	    if GRAMMAR.languages[tgtlanguage].hasLinearization(morph[0]):
		return gf_utils.gf_postprocessor( GRAMMAR.languages[tgtlanguage].linearize( pgf.readExpr(morph[0]) ) );
    return word;

def translationByLookup(sentence):
    global GRAMMAR, SRCLANG, TGTLANGS;
    return [(lang, gf_utils.gf_postprocessor("% " + " ".join([translateWord(word, language) for word in sentence.split()]))) \
	    for lang in TGTLANGS];

def translateWordsAsChunks(word):
    global GRAMMAR, SRCLANG, TGTLANGS;
    parser = GRAMMAR.languages[SRCLANG].parse;
    linearizersList = dict((lang, GRAMMAR.languages[lang].linearize) for lang in TGTLANGS);
    translations = [];
    try:
	for parseidx, parse in enumerate( parser(word) ):
	    for lang in TGTLANGS:
		trans = linearizersList[lang](parse[1]);
		translations.append(( lang, gf_utils.gf_postprocessor(trans.strip() if trans else '') ) );
	    break;
    except pgf.ParseError, err:
	return [];
    return translations;

def translateWord_(word):
    global GRAMMAR, SRCLANG, TGTLANGS;
    possible_translations = translateWordsAsChunks(word);
    if len(possible_translations):
	return possible_translations;
    lowerword = word.lower();
    try:
	partialExprList = GRAMMAR.languages[SRCLANG].parse(word, cat='Chunk');
	for expr in partialExprList:
	    return [(lang, gf_utils.gf_postprocessor( GRAMMAR.languages[lang].linearize(expr[1]) ) ) for lang in TGTLANGS];
    except pgf.ParseError:
	morphAnalysis = GRAMMAR.languages[SRCLANG].lookupMorpho(word) + GRAMMAR.languages[SRCLANG].lookupMorpho(lowerword);
	for morph in morphAnalysis:
	    countPositiveLanguages = filter(None, [GRAMMAR.languages[lang].hasLinearization(morph[0]) for lang in TGTLANGS]);
	    if len(countPositiveLanguages) > 0.5*len(TGTLANGS):
		return [(lang, gf_utils.gf_postprocessor( GRAMMAR.languages[lang].linearize( pgf.readExpr(morph[0]) ) )) for lang in TGTLANGS];
    return [(lang, word) for lang in TGTLANGS];

def translationByLookup_(sentence):
    global GRAMMAR, SRCLANG, TGTLANGS;
    parser = GRAMMAR.languages[SRCLANG].parse;
    linearizersList = dict([(lang, GRAMMAR.languages[lang].linearize) for lang in TGTLANGS]);
    queue = [sentence.strip().split()];
    transChunks = {};
    while len(queue):
	head = queue[0];
	if not len(head):
	    pass;
	elif len(head) == 1 and head[0].strip():
	    for lang, wordchoice in translateWord_(head[0]):
		transChunks.setdefault(lang, []).append( gf_utils.gf_postprocessor(wordchoice) );
	else:
	    try:
		for parseidx, parse in enumerate( parser(' '.join(head)) ):
		    for lang in TGTLANGS:
			if linearizersList[lang](parse[1]) == None:
			    transChunks.setdefault(lang, []).append( ' ' );
			else:
			    transChunks.setdefault(lang, []).append( gf_utils.gf_postprocessor( linearizersList[lang](parse[1]).strip() ) );
		    break;
	    except pgf.ParseError, err:
		#unseenToken = re.findall('"[^"]+?"', err.message)[0][1:-1];
		unseenToken = err.message.strip().split()[-1][1:-1];
		idx = head.index(unseenToken);
		queue.insert(1, head[:idx] );
		queue.insert(2, [head[idx]] );
		queue.insert(3, head[idx+1:] );
	del queue[0];
    for lang in TGTLANGS:
	yield (lang, ' '.join(transChunks[lang]));

def pipelineParsing(sentences):
    global GRAMMAR, SRCLANG;
    #buf = [sent for sent in sentences];
    buf, sentences = itertools.tee(sentences, 2);
    sentences = itertools.imap(gf_utils.gf_lexer(lang=SRCLANG), sentences);
    parser = gf_utils.getKBestParses(GRAMMAR, SRCLANG, 20, callbacks=[("PN", parseNames), ("Symb", parseUnknown)]);
    for sent, (time, parsesBlock) in itertools.izip(buf, itertools.imap(parser, sentences)):
	yield (sent, parsesBlock);

def translation_pipeline(props):
    global GRAMMAR, SRCLANG, TGTLANGS;

    if props.propsfile:
	props = readTranslationPipelineOptions(props.propsfile, props);

    if not os.path.isdir( props.exp_directory ):
	logging.info( "Creating output directory: %s" %(props.exp_directory) );
	#print "Creating output directory: %s" %(props.exp_directory);
	os.makedirs(props.exp_directory);
    
    if not props.srclang:
	logging.critical( "Mandatory option source-lang missing. Can not determine source language." );
	#print "Mandatory option source-lang missing. Can not determine source language.";
	sys.exit(1);
    
    GRAMMAR = pgf.readPGF(props.pgffile);
    
    SRCLANG = filter(None, [lang if lang[-3:] == props.srclang else '' for lang in GRAMMAR.languages.keys()])[0];
    logging.info( "Translating from %s"%(SRCLANG) );
    
    if len(props.tgtlangs):
	target_langs = props.tgtlangs;
    else:
	target_langs = filter(None, [lang[-3:] if lang != SRCLANG else '' for lang in GRAMMAR.languages.keys()]);
    TGTLANGS = filter(None, [lang if lang[-3:] in target_langs else '' for lang in GRAMMAR.languages.keys()]);
    logging.info( "Translating into the following languages: %s"%(','.join(TGTLANGS)) );
    
    if not props.input:
	logging.info( "Input file name missing. Reading input from stdin." );
	inputStream = sys.stdin;
	outputPrefix = os.getpid();
    else:
	inputStream = codecs.open(props.input, 'r');
	outputPrefix = os.path.splitext( os.path.split(props.input)[1] )[0];
    
    if props.format == 'sgm':
	inputDoc    = etree.parse(inputStream);
	reader      = sgmReader;
	skeletonDoc = getXMLSkeleton;
	addItem     = addToSgm;
	writer      = sgmWriter;
    elif props.format == 'txt':
	logging.info( "Input format is txt. Assuming one-sentence-per-line format." );
	inputDoc    = inputStream;
	reader      = lambda X: X;
	skeletonDoc = lambda X, lang: list();
	addItem     = lambda X, y: list.append(X, y); 
	writer      = lambda X: '\n'.join(X);
    
    translationBlocks = {};
    for tgtlang in TGTLANGS+['abstract']:
	translationBlocks[tgtlang] = skeletonDoc(inputDoc, tgtlang);

    preprocessor  = pipeline_lexer;
    postprocessor = lambda X: X #clean_gfstrings;

    logging.info( "Parsing text in %s" %(SRCLANG) );
    # 1. Get Abstract Trees for sentences in source language.
    absParses  = [parsesBlock for parsesBlock in pipelineParsing( itertools.imap(preprocessor, reader(inputDoc)) )];

    logging.info( "Linearizing into %s" %(','.join(TGTLANGS)) );
    # 2. Linearize in all target Languages
    for idx, parsesBlock in enumerate( itertools.imap(operator.itemgetter(1), absParses) ):
	translationBuffer = {};
	if not len(parsesBlock):
	    # failed to parse;
	    # translate using lookup
	    for tgtlang, translation in translationByLookup_(absParses[idx][0]):
		addItem(translationBlocks[tgtlang], postprocessor(translation));
	    addItem(translationBlocks['abstract'], '');
	else:
	    bestTranslationIdx = 0;
	    for tgtlang in TGTLANGS:
		#print "Linearizing into %s" %tgtlang;
		translationBuffer[tgtlang] = gf_utils.getKTranslations(GRAMMAR, tgtlang, [parsesBlock]).next();
		for tidx, translation in enumerate(translationBuffer[tgtlang]):
		    if postprocessor(translation[1]).strip():
			if tidx > bestTranslationIdx:
			    bestTranslationIdx = tidx;
			break;

	    for tgtlang in TGTLANGS:
		translation = translationBuffer[tgtlang][bestTranslationIdx] if len(translationBuffer[tgtlang]) > bestTranslationIdx else ((None,), '');
		addItem(translationBlocks[tgtlang], postprocessor(translation[1]));

	    addItem(translationBlocks['abstract'], str(parsesBlock[bestTranslationIdx][1]));

    for tgtlang in TGTLANGS+['abstract']:
	outputFile = os.path.join( props.exp_directory, '%s-%s.%s' %(outputPrefix, tgtlang[-3:] if tgtlang!='abstract' else 'abstract', props.format) );
	logging.info( "Writing translations for %s to %s" %(tgtlang, outputFile) );
	with codecs.open(outputFile, 'w') as outputStream:
	    print >>outputStream, writer(translationBlocks[tgtlang]);
    return;

def cmdLineParser():
    argparser = argparse.ArgumentParser(prog='translation_pipeline.py', description='Run the GF translation pipeline on standard test-sets');
    argparser.add_argument('-g', '--pgf', dest='pgffile', required=True, help='PGF grammar file to run the pipeline');
    argparser.add_argument('-s', '--source', dest='srclang', default='', help='Source language of input sentences');
    argparser.add_argument('-t', '--target', dest='tgtlangs', nargs='*', default=[], help='Target languages to linearize (default is all other languages)');
    argparser.add_argument('-i', '--input', dest='input', default='', help='input file (default will accept STDIN)');
    argparser.add_argument('-f', '--format', dest='format', default='txt', choices=['txt', 'sgm'], help='input file format (output files will be written in the same format)');
    argparser.add_argument('-e', '--exp', dest='exp_directory', default=os.getcwd(), help='experiement directory to write translation files');
    argparser.add_argument('-p', '--props', dest='propsfile', default='', help='properties file for the translation pipeline (specify the above arguments in a file)');
    return argparser;

if __name__ == '__main__':
    logging.basicConfig(level='INFO');
    pipelineEnv = cmdLineParser().parse_args(sys.argv[1:]);
    translation_pipeline(pipelineEnv);
