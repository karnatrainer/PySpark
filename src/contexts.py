# Prasad's Comment
from pyspark import SparkConf,SparkContext

def create_contexts():
    conf = SparkConf()
    conf.set("spark.master","local")
    conf.set("spark.app.name","sparkapp")
        
    sc = SparkContext(conf=conf)
    return sc
