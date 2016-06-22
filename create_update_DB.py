import boto3
import re
import sqlite3
#from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
from datetime import datetime

start = datetime.now().timestamp()

s3 = boto3.resource('s3')
bucket = s3.Bucket('forgelog')
cols = re.compile(r'(?P<owner>\S+) (?P<bucket>\S+) (?P<time>\[[^]]*\]) (?P<ip>\S+) (?P<requester>\S+) (?P<reqid>\S+) (?P<operation>\S+) (?P<key>\S+) (?P<request>"[^"]*") (?P<status>\S+) (?P<error>\S+) (?P<bytes>\S+) (?P<size>\S+) (?P<totaltime>\S+) (?P<turnaround>\S+) (?P<referrer>"[^"]*") (?P<useragent>"[^"]*") (?P<version>\S)')


def isSQLite3(filename):
    #Check if database exists
    from os.path import isfile, getsize
    if not isfile(filename):
        return False
    if getsize(filename) < 100:
        return False
    with open(filename, 'rb') as fd:
        header = fd.read(100)
    return header[0:16] == b'SQLite format 3\x00'


def chunks(l, n=8):
    #Yield n-sized chunks from l
    for i in range(0, len(l), n):
        yield l[i:i+n]


def str2dict(s, f):
    x = cols.match(s)
    if x is not None:
        x = x.groupdict()
        x.update({'logfile': f})
        return x
    else:
        return


def job(key):
    fileObj = s3.Object(bucket.name, key)
    strs = str(fileObj.get()["Body"].read()).split('\\n')
    group = []
    for oneLine in strs:
        if (len(oneLine) > 3):
            group.append(str2dict(oneLine, key))
    return group


#Create/Connect Database
#Need to get last update
mark = ()

if isSQLite3('s3LogDB.db'):
    conn = sqlite3.connect('s3LogDB.db')
    cur = conn.cursor()
    cur.execute('''SELECT KEY FROM MARKER ORDER BY TIMESTAMP DESC LIMIT 1''')
    mark = cur.fetchone()

else:
    conn = sqlite3.connect('s3LogDB.db')
    cur = conn.cursor()
    cur.execute('''CREATE TABLE log(id INTEGER PRIMARY KEY AUTOINCREMENT, owner TEXT, bucket TEXT, time TEXT, ip TEXT, requester TEXT, reqid TEXT, operation TEXT, key TEXT, request TEXT, status INT, error TEXT, bytes INT, size INT, totaltime INT, turnaround INT, referrer TEXT, useragent TEXT, version TEXT, logfile TEXT)''')
    cur.execute('''CREATE TABLE filelist(id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT)''')
    cur.execute('''CREATE TABLE marker(id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT, timestamp NUMERIC)''')
    conn.commit()

if type(mark) is tuple and len(mark) > 0:
    listOfKeys = [item.key for item in bucket.objects.filter(Marker=mark[0])]
else:
    listOfKeys = [item.key for item in bucket.objects.all()]

rows2add = len(listOfKeys)

if rows2add > 0:
    cur.executemany('INSERT INTO filelist VALUES(null, ?)', list(zip(listOfKeys, )))
    cur.execute('INSERT INTO marker VALUES(null, :key, :timestamp)', {'key': listOfKeys[-1], 'timestamp': datetime.now().timestamp()})
    conn.commit()
    p = ThreadPool(44)
    compiledLog = []
    compiledLog.extend(p.map(job, listOfKeys))
    p.close()
    compiledLog = [item for sublist in compiledLog for item in sublist]
    compiledLog = filter(None, compiledLog)

    cur.executemany('INSERT INTO log VALUES (null, :owner, :bucket, :time, :ip, :requester, :reqid, :operation, :key, :request, :status, :error, :bytes, :size, :totaltime, :turnaround, :referrer, :useragent, :version, :logfile)', compiledLog)

    conn.commit()

conn.close()
print('''Elapsed Time : ''' + str(datetime.now().timestamp()-start))
print("Added : " + str(rows2add) + " Records")
