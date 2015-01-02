
import codecs, sys;

sys.stdin  = codecs.getreader('utf-8')(sys.stdin);
sys.stdout = codecs.getwriter('utf-8')(sys.stdout);

selectSentences = {};
for line in sys.stdin:
    selectSentences[int(line.strip())] = True;

with codecs.open(sys.argv[1], 'r', 'utf-8') as infile:
    line_count = 0;
    for line in infile:
	line_count += 1;
	if selectSentences.get(line_count, False):
	    print line.strip();
