import os
import shutil
import re
import sys
import itertools
import time
from pyspark import SparkConf, SparkContext
filename = sys.argv[1]
output_dir = sys.argv[2]
conf = SparkConf()
sc = SparkContext(conf=conf)

start=time.time()
lines = sc.textFile(filename)
user_id = lines.map(lambda l:re.split(r'[^\w]+', l))
connection_user=user_id.flatMap(lambda u:[((u[0],i),-100) for i in u[1:]])
connection_friend=user_id.flatMap(lambda u:[(i,1) for i in itertools.combinations(u[1:], 2)])
count=connection_user.union(connection_friend).reduceByKey(lambda n1, n2: n1 + n2)
count_groupby=count.map(lambda x:(x[0][0],(x[0][1],x[1]))).groupByKey()
def gettopN(rec,topN):
    x=[]
    for i in sorted(rec[1],key=lambda k: k[1],reverse=True):
        if len(x)>=topN:
            break
        if i[1]>0:
            x.append(i[0])
    return int(rec[0]),x
result=count_groupby.map(lambda x: gettopN(x,10)).sortByKey()
#result=result.sortByKey(keyfunc=int) not working, why? 
if os.path.isdir(output_dir):
    shutil.rmtree(output_dir)
result.saveAsTextFile(output_dir)
end=time.time()
print(end-start)
sc.stop()
