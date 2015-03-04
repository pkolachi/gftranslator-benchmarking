
import codecs, random, sys;

sentences_hash = {};
abstract_hash  = {};

sentencesfile, treesfile = sys.argv[1], sys.argv[2];
line_count = 0;
for srcline, treeline in zip(   codecs.open(sentencesfile, 'r', 'utf-8'), codecs.open(treesfile, 'r', 'utf-8')   ):
    line_count += 1;
    sentences_hash.setdefault(srcline.strip(), []).append(line_count);
    abstract_hash[line_count] = treeline.strip();

for sentence in sentences_hash.keys():
    if len(sentences_hash[sentence]) == 1:
	print sentences_hash[sentence][0];
	pass;
    else:
	#for lid in sentences_hash[sentence]:
	#    print lid, abstract_hash[lid];
	#print random.choice(sentences_hash[sentence]);
	#print "="*30
	pass;
