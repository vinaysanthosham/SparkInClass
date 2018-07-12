import os

os.environ["SPARK_HOME"] = r"C:\Users\Bhave\Desktop\vinayscoop\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]=r"C:\Users\Bhave\Desktop\vinayscoop\winutils"
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()

    lines = sc.textFile(r"C:\Users\Bhave\Desktop\vinayscoop\Spark Python Code\sample", 1)
    


    counts = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: x) \
        .sortByKey()

    counts.saveAsTextFile("output2")
    sc.stop()

