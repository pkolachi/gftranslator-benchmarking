
import codecs, sys;
#import lxml.etree as etree;
import xml.etree.cElementTree as etree;
from translation_pipeline import indentXMLNodes;

getSystemName = lambda xmldoc: xmldoc.getroot().find('.//doc').attrib['sysid'] if len(xmldoc.getroot().findall('.//doc')) == 1 else 'unnamed'

if len(sys.argv) < 5:
    print >>sys.stderr, "./%s <sgm-infile> src-lang tgt-lang <sgm-translation-files>" %sys.argv[0];
    sys.exit(1);

srcfile, srclang, tgtlang, translations = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:];

chosen_ids = [];
if not sys.stdin.isatty():
    for line in sys.stdin:
	chosen_ids.append( int(line.strip()) );
    sys.stdin = open('/dev/tty');
else:
    chosen_ids = None;

indoc  = etree.parse(srcfile);
inroot = indoc.getroot();
setid, srclang = inroot.attrib['setid'], inroot.attrib['srclang'];
# get document id; 
docid = inroot.find('.//doc').attrib['docid'];

transdoc = [etree.parse(doc) for doc in translations];
transdoc = dict([(getSystemName(doc), doc.getroot()) for doc in transdoc]);

finaldoc = etree.Element('set');
finaldoc.attrib['id'] = setid;
finaldoc.attrib['source-language'] = srclang;
finaldoc.attrib['target-language'] = tgtlang;

for seg in inroot.findall('.//seg'):
    if chosen_ids != None and int(seg.attrib['id']) not in chosen_ids:
	continue;

    tgtsegnode = etree.SubElement(finaldoc, 'seg');
    tgtsegnode.attrib['id'] = seg.attrib['id'];
    tgtsegnode.attrib['doc-id'] = docid;

    srcnode = etree.SubElement(tgtsegnode, 'source');
    srcnode.text = seg.text;

    for name, root in transdoc.iteritems():
	translation = '';
	for transseg in root.findall('.//seg'):
	    if transseg.attrib['id'] == seg.attrib['id']:
		translation = transseg.text;
    
	transnode = etree.SubElement(tgtsegnode, 'translation');
	transnode.attrib['system'] = name;
	transnode.text = translation ;

indentXMLNodes(finaldoc);
print etree.tostring(finaldoc, method='xml');
#print etree.tostring(finaldoc, encoding='utf-8', method='xml');
