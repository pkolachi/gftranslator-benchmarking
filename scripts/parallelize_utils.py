
import collections, itertools, multiprocessing, operator, signal, sys;

def timeout(signum, frame):
    raise MemoryError("Function execution timed out");

def pExecFunction(function, queueIn, queueOut, default_value=None):
    while True:
	idx, value = queueIn.get();
	if idx == None:
	    break;
	try:
	    #signal.signal(signal.SIGALRM, timeout);
	    #signal.alarm(5);
	    queueOut.put((idx, function(value)));
	    #signal.alarm(0);
	except MemoryError, err: # deprecated without the signal handler
	    queueOut.put((idx, default_value));
	except multiprocessing.TimeoutError:
	    queueOut.put((idx, default_value));
	except Exception, err:
	    queueOut.put((idx, default_value));

def pExecStarFunction(function, queueIn, queueOut, default_value=None):
    while True:
	idx, value = queueIn.get();
	if idx == None:
	    break;
	try:
	    #signal.signal(signal.SIGALRM, timeout);
	    #signal.alarm(5);
	    queueOut.put((idx, function(*value)));
	    #signal.alarm(0);
	except MemoryError: # deprecated without the signal handler
	    queueOut.put((idx, default_value));
	except multiprocessing.TimeoutError:
	    queueOut.put((idx, default_value));
	except Exception:
	    queueOUt.put((idx, default_value));

def parmap(function, argsList, default_return=None, threads=1, chunksize=500):
    argsList, idxList, jobCount = iter(argsList), itertools.count(0), 0;
    terminateAfterExec, resultsBuffer = False, {};
    inQueue  = multiprocessing.Queue(1);
    outQueue = multiprocessing.Queue();
    subprocesses = [multiprocessing.Process(target=pExecFunction, args=(function, inQueue, outQueue, default_return)) for _ in xrange(threads)];
    for proc in subprocesses:
	proc.daemon = True;
	proc.start();
    curJobCount = 0;
    while True:
	newArgs = [inQueue.put((idx, args)) for idx, args in zip(itertools.islice(idxList, chunksize), itertools.islice(argsList, chunksize))];
	jobCount += len(newArgs);
	curJobCount += len(newArgs);
	if len(newArgs) < chunksize:
	    [inQueue.put((None, None)) for _ in xrange(threads)];
	    terminateAfterExec = True; 
	print >>sys.stderr, "Feeding %d jobs to the queue, now %d jobs in the queue" %(len(newArgs), curJobCount);
	resultsList = [];
	if terminateAfterExec:
	    while len(resultsList) < curJobCount:
		resultsList.append( outQueue.get() );
	    [proc.join() for proc in subprocesses];
	else:
	    while len(resultsList) < 0.85*curJobCount:
		resultsList.append( outQueue.get() );
	curJobCount -= len(resultsList);
	print >>sys.stderr, "Finished %d jobs in the queue, now %d jobs in the queue" %(len(resultsList), curJobCount);
	resultsBuffer.update( dict((idx, value) for (idx, value) in resultsList) );    
	if terminateAfterExec:
	    break;
    return [resultsBuffer.get(idx, default_return) for idx in xrange(jobCount)];

def parstarmap(function, argsList, default_return=None, threads=1, chunksize=500):
    argsList, idxList, jobCount = iter(argsList), itertools.count(0), 0;
    terminateAfterExec, resultsBuffer = False, {};
    inQueue  = multiprocessing.Queue(1);
    outQueue = multiprocessing.Queue();
    subprocesses = [multiprocessing.Process(target=pExecStarFunction, args=(function, inQueue, outQueue)) for _ in xrange(threads)];
    for proc in subprocesses:
	proc.daemon = True;
	proc.start();
    curJobCount = 0;
    while True:
	newArgs = [inQueue.put((idx, args)) for idx, args in zip(itertools.islice(idxList, chunksize), itertools.islice(argsList, chunksize))];
	jobCount += len(newArgs);
	curJobCount += len(newArgs);
	if len(newArgs) < chunksize:
	    [inQueue.put((None, None)) for _ in xrange(threads)];
	    terminateAfterExec = True;
	print >>sys.stderr, "Feeding %d jobs to the queue, now %d jobs in the queue" %(len(newArgs), curJobCount);
	resultsList = [];
	if terminateAfterExec:
	    while len(resultsList) < curJobCount:
		resultsList.append( outQueue.get() );
	    [proc.join() for proc in subprocesses];
	else:
	    while len(resultsList) < 0.85*curJobCount:
		resultsList.append( outQueue.get() );
	curJobCount -= len(resultsList);
	print >>sys.stderr, "Finished %d jobs in the queue, now %d jobs in the queue" %(len(resultsList), curJobCount);
	resultsBuffer.update( dict((idx, value) for (idx, value) in resultsList) );
	if terminateAfterExec:
	    break;
    return [resultsBuffer.get(idx, default_return) for idx in xrange(jobCount)];

def parimap(function, argsList, default_return=None, threads=1, chunksize=500):
    argsList, idxList, jobCount = iter(argsList), itertools.count(0), 0;
    terminateAfterExec, iterIdx, resultsBuffer = False, 0, {};
    inQueue  = multiprocessing.Queue(1);
    outQueue = multiprocessing.Queue();
    subprocesses = [multiprocessing.Process(target=pExecFunction, args=(function, inQueue, outQueue, default_return)) for _ in xrange(threads)];
    for proc in subprocesses:
	proc.daemon = True;
	proc.start();
    curJobCount = 0;
    while True:
	newArgs = [inQueue.put((idx, args)) for idx, args in zip(itertools.islice(idxList, chunksize), itertools.islice(argsList, chunksize))];
	jobCount += len(newArgs);
	curJobCount += len(newArgs);
	if len(newArgs) < chunksize:
	    [inQueue.put((None, None)) for _ in xrange(threads)];
	    terminateAfterExec = True; 
	print >>sys.stderr, "Feeding %d jobs to the queue, now %d jobs in the queue" %(len(newArgs), curJobCount);
	resultsList = [];
	if terminateAfterExec:
	    while len(resultsList) < curJobCount:
		resultsList.append( outQueue.get() );
	    [proc.join() for proc in subprocesses];
	else:
	    while len(resultsList) < 0.85*curJobCount:
		resultsList.append( outQueue.get() );
	curJobCount -= len(resultsList);
	print >>sys.stderr, "Finished %d jobs in the queue, now %d jobs in the queue" %(len(resultsList), curJobCount);
	resultsBuffer.update( dict((idx, value) for (idx, value) in resultsList) );
	for idx in xrange(iterIdx, jobCount):
	    if not resultsBuffer.has_key(idx):
		iterIdx = idx;
		break;
	    yield resultsBuffer[idx];
	    del resultsBuffer[idx];
	if terminateAfterExec:
	    break;
    for idx in xrange(iterIdx, jobCount):
	yield resultsBuffer.get(idx, default_return);
	del resultsBuffer[idx];

def paristarmap(function, argsList, default_return=None, threads=1, chunksize=500):
    argsList, idxList, jobCount = iter(argsList), itertools.count(0), 0;
    terminateAfterExec, iterIdx, resultsBuffer = False, 0, {};
    inQueue  = multiprocessing.Queue(1);
    outQueue = multiprocessing.Queue();
    subprocesses = [multiprocessing.Process(target=pExecStarFunction, args=(function, inQueue, outQueue)) for _ in xrange(threads)];
    for proc in subprocesses:
	proc.daemon = True;
	proc.start();
    curJobCount = 0;
    while True:
	newArgs = [inQueue.put((idx, args)) for idx, args in zip(itertools.islice(idxList, chunksize), itertools.islice(argsList, chunksize))];
	jounCount += len(newArgs);
	curJobCount += len(newArgs);
	if len(newArgs) < chunksize:
	    [inQueue.put((None, None)) for _ in xrange(threads)];
	    terminateAfterExec = True; 
	print >>sys.stderr, "Feeding %d jobs to the queue, now %d jobs in the queue" %(len(newArgs), curJobCount);
	resultsList = [];
	if terminateAfterExec:
	    while len(resultsList) < curJobCount:
		resultsList.append( outQueue.get() );
	    [proc.join() for proc in subprocesses];
	else:
	    while len(resultsList) < 0.85*curJobCount:
		resultsList.append( outQueue.get() );
	curJobCount -= len(resultsList);
	print >>sys.stderr, "Finished %d jobs in the queue, now %d jobs in the queue" %(len(resultsList), curJobCount);
	resultsBuffer.update( dict((idx, value) for (idx, value) in resultsList) );
	for idx in xrange(iterIdx, jobCount):
	    if not resultsBuffer.has_key(idx):
		iterIdx = idx;
		break;
	    yield resultsBuffer[idx];
	    del resultsBuffer[idx];
	if terminateAfterExec:
	    break;
    for idx in xrange(iterIdx, jobCount):
	yield resultsBuffer.get(idx, default_return);
	del resultsBuffer[idx];
