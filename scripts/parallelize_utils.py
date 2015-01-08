
import collections, itertools, multiprocessing, operator;

def pExecFunction(function, queueIn, queueOut):
    while True:
	idx, value = queueIn.get();
	if idx == None:
	    break;
	res = function(value);
	queueOut.put((idx, res));

def parmap(function, argsList, threads=1, chunksize=10000):
    if isinstance(argsList, collections.Iterable):
	argsList = tuple(args for args in argsList);
    jobCount = len(argsList);

    curIdx = 0;
    fullResultsList = [];
    
    while curIdx < jobCount:
	inQueue  = multiprocessing.Queue(1);
	outQueue = multiprocessing.Queue();
	
	subprocesses = [multiprocessing.Process(target=pExecFunction, args=(function, inQueue, outQueue)) for _ in xrange(threads)];
	for proc in subprocesses:
	    proc.daemon = True;
	    proc.start();
	    
	localArgs = [inQueue.put((idx, args)) for idx, args in itertools.izip(xrange(curIdx, curIdx+chunksize), argsList[curIdx:curIdx+chunksize])];
	localJobCount = len(localArgs);
	[inQueue.put((None, None)) for _ in xrange(threads)];
	resultsList = [outQueue.get() for _ in xrange(localJobCount)];
	[proc.join() for proc in subprocesses];
	
	fullResultsList += [result for idx, result in sorted(resultsList, key=operator.itemgetter(0))];
	
	curIdx += chunksize;

    return fullResultsList;

def parimap(function, argsList, threads=1, chunksize=10000):
    if isinstance(argsList, collections.Iterable):
	argsList = tuple(args for args in argsList);
    jobCount = len(argsList);

    curIdx = 0;

    while curIdx < jobCount:
	inQueue  = multiprocessing.Queue(1);
	outQueue = multiprocessing.Queue();
	
	subprocesses = [multiprocessing.Process(target=pExecFunction, args=(function, inQueue, outQueue)) for _ in xrange(threads)];
	for proc in subprocesses:
	    proc.daemon = True;
	    proc.start();

	localArgs = [inQueue.put((idx, args)) for idx, args in itertools.izip(xrange(curIdx, curIdx+chunksize), argsList[curIdx:curIdx+chunksize])];
	localJobCount = len(localArgs);
	[inQueue.put((None, None)) for _ in xrange(threads)];
	resultsList = [outQueue.get() for _ in xrange(localJobCount)];
	[proc.join() for proc in subprocesses];

	for idx, result in sorted(resultsList, key=operator.itemgetter(0)):
	    yield result;

	curIdx += chunksize;

    return;

def parstarmap(function, argsList, threads=1, chunksize=100):
    pass;

def paristarmap(function, argsList, threads, chunksize=100):
    pass;
