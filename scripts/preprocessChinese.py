import codecs, string, sys;

sys.stdout = codecs.getwriter('utf-8')(sys.stdout);

for line in sys.stdin:
    line = line.strip().decode('utf-8')
    tokens, idx, n = [], 0, len(line);
    prev = True;
    while idx < n:
	if line[idx] in string.whitespace:
	    prev = True;
	    idx += 1;
	    continue;
	if 0 < ord(line[idx]) < 128:
	    if line[idx] in string.punctuation:
		prev = True;
	    if prev:
		tokens.append( line[idx] );
		prev = False;
	    else:
		tokens[-1] = tokens[-1]+line[idx];
	else:
	    prev = True;
	    tokens.append( line[idx] );
	idx += 1;
    print ' '.join(tokens);
