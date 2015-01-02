
import codecs, sys;

def readSentencesFromTextFile(filename):
    with codecs.open(filename, 'r', 'utf-8') as infile:
	for line in infile:
	    yield line.strip();

if len(sys.argv) < 5:
    print >>sys.stderr, "./%s input-file reference-translations hypothesis1-annotations hypothesis2-annotations";
    sys.exit(1);

inputFile = sys.argv[1];
referenceFile = sys.argv[2];
annotationFile1, annotationFile2 = sys.argv[3], sys.argv[4];

inputIter = [sent for sent in readSentencesFromTextFile(inputFile)];
referenceIter = [sent for sent in readSentencesFromTextFile(referenceFile)];
ann1Iter = [line for line in readSentencesFromTextFile(annotationFile1)];
ann2Iter = [line for line in readSentencesFromTextFile(annotationFile2)];

hypTypes = {};
stepFunction = lambda x: -1 if x < 0 else 1 if x > 0 else 0
for hyp1, hyp2 in zip(ann1Iter, ann2Iter):
    try:
	bleu1, sid1, hyp1, ref1 = hyp1.split('\t');
	bleu2, sid2, hyp2, ref2 = hyp2.split('\t');
    except ValueError:
	print >>sys.stderr, hyp1
	print >>sys.stderr, hyp2
	continue;
    assert(sid1 == sid2);
    assert(ref1 == ref2);
    diff = float(bleu1)-float(bleu2);
    key = stepFunction(diff);
    hypTypes.setdefault(key, {})[int(sid1)] = (abs(diff), max(float(bleu1), float(bleu2)));

sys.stdout = codecs.getwriter('utf-8')(sys.stdout);

for hT in (0, -1, 1):
    print "No changes" if hT == 0 else "Improved translations" if hT == -1 else "Impaired translations";
    print "%d sentences in this category" %(len(hypTypes[hT]));
    for sid, diffscore in sorted(hypTypes[hT].iteritems(), key=lambda (k,v):(v,k), reverse=True):
	print "SOURCE_SENTENCE\t%d\t%s" %(sid, inputIter[sid]);
	print "REFERENCE\t%s" %(referenceIter[sid]);
	print "%s\t%s" %(ann1Iter[sid].split('\t')[0], ' '.join([tok.split('|')[0] for tok in ann1Iter[sid].split('\t')[2].split()]));
	print "%s\t%s" %(ann2Iter[sid].split('\t')[0], ' '.join([tok.split('|')[0] for tok in ann2Iter[sid].split('\t')[2].split()]));
    print "\n"+"="*80+"\n";
