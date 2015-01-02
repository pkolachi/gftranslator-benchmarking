
import codecs, sys;

if len(sys.argv) != 2:
    print >>sys.stderr, "./%s <failed-sent-ids>";
    sys.exit(1);

failedIds = {};
with codecs.open(sys.argv[1], 'r', 'utf-8') as failedsentences:
    for line in failedsentences:
	failedIds[int(line.strip())] = True;
							    
line_idx = 0;
for line in sys.stdin:
    line_idx += 1;
    while failedIds.has_key(line_idx):
	#print line_idx, "";
	print "";
	line_idx += 1;
    #print line_idx, line.strip();
    print line.strip();
line_idx += 1;
while failedIds.has_key(line_idx):
    #print line_idx, "";
    print "";
    line_idx += 1;

