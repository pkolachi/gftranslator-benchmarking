#!/usr/bin/env python

import codecs, os, sys;
#import lxml.etree as etree;
import xml.etree.cElementTree as etree;

def parseTaskXMLFile(filepath):
    ## PROCESSING TASK XML
    source_sentences = dict();
    system_hypothesis = dict();
    reference_translations = dict();
    indoc = etree.parse(filepath);
    inroot = indoc.getroot();
    dataset_name, source_lang, target_lang = inroot.attrib['id'], inroot.attrib['source-language'], inroot.attrib['target-language'];
    for seg in inroot.findall('.//seg'):
	key = (seg.attrib['doc-id'], seg.attrib['id']);
	source_sentences[key] = seg.find('source').text;
	for hypothesis in seg.findall('translation'):
	    system_name = hypothesis.attrib['system'];
	    system_hypothesis.setdefault(system_name, dict())[key] = hypothesis.text;
    return dataset_name, source_lang, target_lang, source_sentences, reference_translations, system_hypothesis;

def parseResultsXMLFile(filepath):
    indoc = etree.parse(filepath);
    inroot = indoc.getroot();
    result_node, task_type = None, "";
    ref_dataset_name, ref_source_lang, ref_target_lang = "", "", "";
    for node in list(inroot):
	if node.tag.endswith('result'):
	    result_node = node;
	    task_type = node.tag;
	    ref_dataset_name = node.attrib['id'];
	    ref_source_lang = node.attrib['source-language'];
	    ref_target_lang = node.attrib['target-language'];
	    break;
    reference_translations = dict();
    for item in list(result_node):
	key = (item.attrib['doc-id'], item.attrib['id'], item.attrib['user']);
	metaInfo = (item.attrib['user'], item.attrib['duration']);
	reference_translations[key] = (list(item)[0].attrib['system'], list(item)[0].text, metaInfo);

    return ref_dataset_name, ref_source_lang, ref_target_lang, reference_translations;

def prepareScoringData(outputDirectory, outputPrefix, sourceLang, targetLang, sourceSegments, systemTranslations, manualTranslations):
    if not os.path.isdir(outputDirectory):
	os.makedirs(outputDirectory);
    # write source sentences, reference translations, hypothesis translations, and meta data
    segment_keys = sorted(sourceSegments.keys());
    source_file = os.path.join(outputDirectory, "%s.raw.%s" %(outputPrefix, sourceLang));
    with codecs.open(source_file, 'w', 'utf-8') as outfile:
	for seg in segment_keys:
	    print >>outfile, sourceSegments[seg];
    reference_file = os.path.join(outputDirectory, "%s.raw.%s" %(outputPrefix, targetLang));
    with codecs.open(reference_file, 'w', 'utf-8') as outfile:
	for seg in segment_keys:
	    print >>outfile, manualTranslations[seg][1];
    metainfo = os.path.join(outputDirectory, "%s.metaInfo"%outputPrefix);
    with codecs.open(metainfo, 'w', 'utf-8') as outfile:
	for seg in segment_keys:
	    print >>outfile, "%s\t%s\t%s\t%s" %('-'.join(seg), manualTranslations[seg][0], manualTranslations[seg][-1][0], manualTranslations[seg][-1][1]);
    for sysname in systemTranslations.keys():
	hypothesis_file = os.path.join(outputDirectory, "%s.%s.raw.%s" %(outputPrefix, sysname, targetLang));
	with codecs.open(hypothesis_file, 'w', 'utf-8') as outfile:
	    for seg in segment_keys:
		print >>outfile, systemTranslations[sysname].get(seg, "");
    return;

def main():
    if len(sys.argv) < 4:
	print >>sys.stderr, "./%s <evaluation-task-xml> <results-xml> <data-directory>" %sys.argv[0];
	sys.exit(1);
	
    evaluationtask_xml, results_xml, dataDirectory = sys.argv[1], sys.argv[2], sys.argv[3];
    dataset_name, source_lang, target_lang, source_sentences, reference_translations, system_hypothesis = parseTaskXMLFile(evaluationtask_xml);
    '''
    for key in source_sentences.keys():
	print source_sentences[key];
	for sysname in sorted(system_hypothesis.keys()):
	    print "\t%s\t%s" %(sysname, system_hypothesis[sysname].get(key, "NONE"));
    '''

    ## PROCESSING RESULT XML
    ref_dataset_name, ref_source_lang, ref_target_lang, gold_translations = parseResultsXMLFile(results_xml);
    
    ## HANDLE MULTIPLE GOLD TRANSLATIONS
    ## TENTATIVELY DESIGNED, NEED TO MODIFY
    single_reference_translations = {};
    user_stats = {};
    for result in gold_translations:
	user_stats[result[-1]] = user_stats.get(result[-1], 0)+1;
    result_count = sum(user_stats.values());
    for user in user_stats:
	user_stats[user] = float(user_stats[user])/result_count;
    for result in gold_translations:
	print result;
	if single_reference_translations.has_key(result[:-1]):
	    # pass;
	    existing_reference = single_reference_translations[result[:-1]];
	    if user_stats[existing_reference[0]] < user_stats[result[-1]]:
		single_reference_translations[result[:-1]] = (result[-1], gold_translations[result]);
	else:
	    single_reference_translations[result[:-1]] = (result[-1], gold_translations[result]);
    for result in single_reference_translations:
	single_reference_translations[result] = single_reference_translations[result][1];

    ## PREPARING SCORING DATA
    prepareScoringData(dataDirectory, dataset_name, source_lang, target_lang, source_sentences, system_hypothesis, single_reference_translations);
    return;

if __name__ == '__main__':
    main()  
