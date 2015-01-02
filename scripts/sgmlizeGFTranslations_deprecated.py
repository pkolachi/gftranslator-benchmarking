
import codecs, copy, re, sys;
from stanford_corenlp_utils import indentXMLNodes;
#import lxml.etree as etree;
import xml.etree.ElementTree as etree;

def getInputDocSkeleton(xmlfile):
    doc  = etree.parse(xmlfile);
    root = doc.getroot();
    for node in root.findall('.//seg'):
	node.text = '';
    return doc;

def readGFTrees(gftranslatefile):
    probexpr = re.compile('\[[0-9.]+\]');
    with codecs.open(gftranslatefile, 'r', 'utf-8') as infile:
	sentences = [];
	cur_entry = [];
	for line in infile:
	    if line.startswith('>'):
		if not cur_entry == []:
		    sentences.append(cur_entry);
		    cur_entry = [];
		if line.strip().startswith('> Unexpected'):
		    sentences.append(cur_entry);
		elif line.strip() == '>':
		    pass;
		else:
		    cur_entry.insert( 0, float(line.strip().split()[1]) );
	    elif re.match(probexpr, line.strip()):
		prob  = re.findall(probexpr, line.strip())[0];
		parse = line.strip().replace(prob, '').strip();
		cur_entry.insert( 1, float(prob[1:-1]) );
		cur_entry.insert( 2, parse.strip() );
    return sentences;

def cleanGFString(string):
    if string.startswith('* '):
	string = string[2:];
    string = string.replace(' &+ ', '');
    string = string.replace('<+>', ' ');
    absFuncName = re.compile('\[[a-zA-Z0-9_]+\]');
    missingEntries = {};
    for entry in re.findall(absFuncName, string):
	missingEntries[entry] = missingEntries.setdefault(entry, 0)+1;
    for entry in missingEntries:
	while missingEntries[entry] > 1:
	    string = string.replace(entry, '');
	    missingEntries[entry] -= 1;
	string = string.replace(entry, entry[1:-1].rsplit('_', 1)[0]);
    return ' '.join( string.split() );

def readGFRunOut(gftranslatefile):
    with codecs.open(gftranslatefile, 'r', 'utf-8') as infile:
	translations, begin = {}, True;
	for line in infile:
	    if not line.strip():
		begin = True;
	    if line.strip():
		lang, src = line.strip().split(':', 1);
		if begin:
		    translations.setdefault(lang, []).append( src.strip() );
		    begin = False;
		else:
		    translations.setdefault(lang, []).append( cleanGFString(src.strip()) );
    return translations;

def sgmlizeGFTranslations(translations, abstracttrees, sgmldoc, sgmlprefix):
    abstreesidx = {};
    idx = 1;
    for trees in abstracttrees:
	if len(trees):
	    abstreesidx[trees[2]] = idx;
	idx += 1;
    absname = filter(None, [name if len(name) == min([len(l) for l in translations.keys()]) else '' for name in translations.keys()])[0];
    for lang in translations.keys():
	if lang == absname:
	    continue;
	clang = lang.replace(absname, '');
	with codecs.open('%s-%s.sgm' %(sgmlprefix, clang.lower()), 'w', 'utf-8') as outfile:
	    curdoc = copy.deepcopy(sgmldoc);
	    curtranslations = {};
	    for abstree, trans in zip(translations[absname], translations[lang]):
		if not abstreesidx.has_key(abstree):
		    continue;
		else:
		    curtranslations[ abstreesidx[abstree] ] = trans;
	    root = curdoc.getroot();
	    for segnode in root.findall('.//seg'):
		if curtranslations.has_key( int(segnode.attrib['id']) ):
		    segnode.text = ' %s ' %curtranslations[ int(segnode.attrib['id']) ];
		if not segnode.text.strip():
		    segnode.text = ' ';
	    root.tag = 'tstset';
	    root.attrib['trglang'] = clang.lower();
	    root.find('doc').attrib['sysid'] = absname;
	    indentXMLNodes(root);
	    print >>outfile, etree.tostring(root, encoding='utf-8', method='xml').decode('utf-8');
    return;

if len(sys.argv) < 5:
    print >>sys.stderr, "./%s <input-sgm> <gf-trees> <gf-translations> <output-sgm-prefix>" %sys.argv[0];
    sys.exit(1);

sgmfile, gftrees, gftranslations, sgmfileprefix = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4];
indoc = getInputDocSkeleton(sgmfile);
gfabstractTrees = readGFTrees(gftrees);
gfTranslations  = readGFRunOut(gftranslations);
sgmlizeGFTranslations(gfTranslations, gfabstractTrees, indoc, sgmfileprefix);
print >>sys.stderr, " ".join( gfTranslations.keys() );
