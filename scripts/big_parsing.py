#!/usr/bin/env python

import codecs, itertools, sys;

sys.stdin  = codecs.getreader('utf-8')(sys.stdin);
sys.stdout = codecs.getwriter('utf-8')(sys.stdout);
sys.stderr = codecs.getwriter('utf-8')(sys.stderr);

def large_parser(*args):
    import pgf;
    import gf_utils, parallelize_utils;
    
    grammarfile, start_cat, lang = args[0], args[1], args[2];
    #inputSet = itertools.imap(gf_lexer('translator'), codecs.open(args[4], 'r') if len(args) > 4 else sys.stdin);
    grammar = pgf.readPGF(grammarfile);
    
    import translation_pipeline_v3;
    callbacks = [('PN', translation_pipeline_v3.parseNames(grammar, lang)), ('Symb', translation_pipeline_v3.parseUnknown(grammar, lang))];

    inputSet = gf_utils.web_lexer(grammar, lang, codecs.open(args[3], 'r') if len(args) > 3 else sys.stdin);
    outputPrinter = lambda X: '%f\t%s' %(X[0][0], X[0][1]) if len(X) else ''; #printJohnsonRerankerFormat;
    parser = gf_utils.getKBestParses(grammar, lang, 1, callbacks=callbacks, serializable=True);

    sentidx = 0;
    with (codecs.open(args[4], 'w') if len(args) > 4 else sys.stdout) as outputStream:
	#for time, parsesBlock in itertools.imap(parser, inputSet):
	for time, parsesBlock in parallelize_utils.parimap(parser, inputSet, default_return=[], threads=3, chunksize=101):
	    sentidx += 1;
	    strParses = str(outputPrinter(parsesBlock));
	    print >>outputStream, "%d\t%.4f\t%s" %(sentidx, time, strParses);
    return;

def merge_parses(*args):
    offset = 0;
    for filepath in args:
	with codecs.open(filepath, 'r', 'utf-8') as infile:
	    newoffset = 0;
	    count = 0;
	    for line in infile:
		count += 1;
		sentidx, time, parseStr = line.strip('\n').split('\t', 2);
		if parseStr.strip():
		    prob, parseExpr = parseStr.split('\t')[0:2];
		else:
		    prob, parseExpr = "", "";
		newsentidx = offset+count;
		print "%d\t%s\t%s\t%s" %(newsentidx, time, prob, parseExpr);
		newoffset = newsentidx;
	    offset = newoffset;
    return;

if __name__ == '__main__':
    large_parser(*sys.argv[1:]);
    #merge_parses(*sys.argv[1:]);
