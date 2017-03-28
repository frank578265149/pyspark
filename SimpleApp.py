""""SimpleApp.py"""

from pyspark import SparkContext,SparkConf
from operator import  add
app_name="pyspark"

sc = SparkContext(master="local",appName=app_name)
broadcastAList=sc.broadcast(list(["a","b","c","d"]))
songs_data = sc.textFile(r"/home/frank/PycharmProjects/write-excel/README")
sc.parallelize(list(["1","2","3"])).map(lambda  s:broadcastAList.value).collect()
count=songs_data.map(lambda  line:len(line))
print(songs_data.first())
print(count)
