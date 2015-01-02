
import codecs, collections, itertools, math, operator, sys;
import conll_utils, random_utils;

sys.stdout = codecs.getwriter('utf-8')(sys.stdout);
sys.stderr = codecs.getwriter('utf-8')(sys.stderr);

def get_tag_mapping(mapfile):
    mapList = {};
    with codecs.open(mapfile, 'r', 'utf-8') as infile:
	for line in infile:
	    tag, mtag = line.strip().split();
	    mapList[tag] = mtag;
    return mapList;

#GOOGLE_postags = get_tag_mapping('en-ptb.map');
GOOGLE_postags = get_tag_mapping('fi-tdt.map');
#postags_of_interest = ['ADJ', 'ADP', 'ADV', 'CONJ', 'DET', 'NOUN', 'NUM', 'PRON', 'PRT', 'VERB', 'X'];
postags_of_interest = ['ADJ', 'ADV', 'NOUN', 'VERB'];

def leafancestors_fromcparse(parsestr):
    n, idx = len(parsestr), 0;
    nonterm_open = False;
    open_nonterminals = [];
    while idx < n:
	if parsestr[idx] == '(':
	    open_nonterminals.append(parsestr[idx+1:].split(' ', 1)[0]);
	    nonterm_open = True;
	elif parsestr[idx] == ')' and nonterm_open == True:
	    yield tuple(open_nonterminals[1:]);
	    nonterm_open = False;
	    del open_nonterminals[-1];
	elif parsestr[idx] == ')' and nonterm_open == False:
	    del open_nonterminals[-1];
	idx += 1;
    return;

def leafancestors_fromdparse(conll_sentence):
    for idx, edge in enumerate(conll_sentence):
	deprels, cidx = [edge['postag']], idx;
	while cidx >= 0:
	    if conll_sentence[cidx]['head'] == '_':
		return;
	    deprels.append( conll_sentence[cidx]['deprel'] );
	    cidx = int(conll_sentence[cidx]['head'])-1;
	yield tuple(reversed(deprels));

def deduplicate(iterator):
    cur_elem, mod_iterator = None, [];
    for elem in iterator:
	if elem != cur_elem:
	    mod_iterator.append(elem);	    
	cur_elem = elem;
    return tuple(mod_iterator);

def getLexicalProfiles(conll_sentence):
    global GOOGLE_postags, postags_of_interest;
    for edge in conll_sentence:
	lexprofile = (edge['lemma'].lower() if edge['lemma'] != '_' else edge['form'].lower(), GOOGLE_postags.get(edge['postag'], edge['postag']));
	if lexprofile[1] in postags_of_interest and edge['feats'].find('nertype') == -1:
	    yield lexprofile;

def getSyntacticProfiles(conll_sentence):
    global GOOGLE_postags, postags_of_interest;
    parse = '';
    for edge in conll_sentence:
	parse += edge['postag'].replace('*', '(%s %s)'%(edge['postag'], edge['form'])).replace('_', ' ');
    if 0:
	for path in leafancestors_fromcparse(parse):
	    synprofile = (deduplicate(path[:-1]), GOOGLE_postags.get(path[-1], path[-1]));
	    if synprofile[1] in postags_of_interest:
		yield synprofile;
    else:
	for path in leafancestors_fromdparse(conll_sentence):
	    synprofile = (deduplicate(path[:-1]), GOOGLE_postags.get(path[-1], path[-1]));
	    if synprofile[1] in postags_of_interest:
		yield synprofile;

conll_sentence_to_raw = lambda conll_sentence: " ".join([token['form'] for token in conll_sentence]);
nerCount              = lambda conll_sentence: sum(1 if edge['form'][0].upper() == edge['form'][0] else 0 for edge in conll_sentence[1:]);
#nerCount              = lambda conll_sentence: sum([1 if edge['feats'].find('nertype') != -1 else 0 for edge in conll_sentence]);

def feat_extract(conll_sentence):
    feat = {};
    optimal_length = 15;
    length = len(conll_sentence);
    rel_length = abs( float(optimal_length-length)/optimal_length )+0.01;
    feat['length'] = rel_length;
    feat['nertypes'] = nerCount(conll_sentence);
    feat['lexprofiles'] = [];
    feat['synprofiles'] = [];
    for lexfeat in getLexicalProfiles(conll_sentence):
	feat['lexprofiles'].append(lexfeat);
    for synfeat in getSyntacticProfiles(conll_sentence):
	feat['synprofiles'].append(synfeat);
    return (conll_sentence_to_raw(conll_sentence), feat);

def extractFeatures(conll_sentences):
    for sentence,featvec in itertools.imap(feat_extract, conll_sentences):
	yield featvec;

def computeRanks(frequencyList):
    ranks, rankid, prevFreq = {}, 0, -1;
    for key, value in sorted(frequencyList.iteritems(), key=lambda (k, v):(v, k), reverse=True):
	if value != prevFreq:
	    rankid += 1;
	ranks[key] = rankid;
    return ranks;

def computeProbDistribution(frequencyList):
    marginalSum = sum(frequencyList.values())+len(frequencyList.values());
    probs = {};
    for key, value in frequencyList.iteritems():
	probs[key] = float(value+1)/marginalSum;
    return probs;

def pruneRanks(ranks, tags_of_interest):
    pranks = {};
    for key, value in ranks.iteritems():
	if key[1] in tags_of_interest:
	    pranks[key] = value;
    return pranks;

def trainRankingModels():
    inputFileName = sys.argv[1] if len(sys.argv) > 1 else '';
    with random_utils.smart_open(inputFileName) as trainfile:
	lfreq, sfreq = collections.defaultdict(int), collections.defaultdict(int);
	for featureinstance in extractFeatures(conll_utils.sentences_from_conll(trainfile)):
	    for feat in featureinstance['lexprofiles']:
		lfreq[feat] += 1;
	    for feat in featureinstance['synprofiles']:
		sfreq[feat] += 1;
    lranks = computeRanks(lfreq);
    sranks = computeRanks(sfreq);
    lranks = pruneRanks(lranks, postags_of_interest);
    sranks = pruneRanks(sranks, postags_of_interest);
    for k, v in sorted(lranks.iteritems(), key=operator.itemgetter(1)):
	print "L\t%s\t%s\t%d" %(k[0], k[1], v);
    for k, v in sorted(sranks.iteritems(), key=operator.itemgetter(1)):
	print "S\t%s\t%s\t%d" %('-'.join(k[0]), k[1], v);
    return;

def readRanks(ranksFile):
    lranks, sranks = {}, {};
    count = 0;
    for rankeditem in random_utils.lines_from_file(ranksFile):
	fields = rankeditem.strip().split('\t');
	if fields[0] == 'L':
	    lranks[(fields[1], fields[2])] = int(fields[3]);
	elif fields[0] == 'S':
	    sranks[(tuple(fields[1].split('-')), fields[2])] = int(fields[3]);
    return lranks, sranks;

def computeWeightedRank(featurevector, lexranks, synranks):
    worstcase = math.log(sys.maxint, 10);
    featweights, featvals = {'length': 0.2, 'lexprofiles': 2, 'synprofiles': 1, 'nertypes': 2}, {'length': worstcase, 'lexprofiles': worstcase, 'synprofiles': worstcase, 'nertypes': worstcase};
    for featclass in featurevector:
	if featclass == 'length':
	    featvals[featclass] = featurevector[featclass];
	elif featclass == 'nertypes':
	    featvals[featclass] = featurevector[featclass]+1;
	elif featclass == 'lexprofiles':
	    for lexitem in featurevector[featclass]:
		featvals[featclass] = featvals.setdefault(featclass, 0) +\
			(worstcase if not lexranks.has_key(lexitem) else math.log(lexranks[lexitem], 10));
	elif featclass == 'synprofiles':
	    for synitem in featurevector[featclass]:
		featvals[featclass] = featvals.setdefault(featclass, 0) +\
			(worstcase if not synranks.has_key(synitem) else math.log(synranks[synitem], 10));
    #totalscore = sum([featweights[featclass]*featvals[featclass] for featclass in featweights]);
    #totalscore = featvals['length']*sum([featweights[featclass]*featvals[featclass] for featclass in ['synprofile', 'lexeme']]);
    totalscore = sum([featweights[featclass]*featvals[featclass] for featclass in ['synprofiles', 'lexprofiles']])*featvals['nertypes']*(featvals['length']*featweights['length']);
    return (totalscore, [featvals[featclass] for featclass in ['length', 'nertypes', 'lexprofiles', 'synprofiles']])

def estimateCandidateRanks():
    lranks, sranks = readRanks( sys.argv[1] );
    testFileName = sys.argv[2] if len(sys.argv) > 2 else '';
    with random_utils.smart_open(testFileName) as testfile:
	for sentence, featvec in itertools.imap(feat_extract, conll_utils.sentences_from_conll(testfile)):
	    score, ind_vals = computeWeightedRank(featvec, lranks, sranks);
	    print "%.4f\t%s" %(score, sentence);
	    print >>sys.stderr, "%.4f\t%s\t%s" %(score, ",".join(["%3.3f"%(val) for val in ind_vals]), sentence);
    return;

if __name__ == '__main__':
    trainRankingModels();
    #estimateCandidateRanks();
