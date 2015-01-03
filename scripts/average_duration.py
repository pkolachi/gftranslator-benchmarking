
import datetime, sys;

durations = [];
for line in sys.stdin:
    line = line.strip();
    dt = datetime.datetime.strptime(line, '%H:%M:%S.%f')
    durations.append( datetime.timedelta( seconds=dt.second, microseconds=dt.microsecond, minutes=dt.minute, hours=dt.hour ) );
print ( sum(durations, datetime.timedelta(0)) ) // len(durations);

