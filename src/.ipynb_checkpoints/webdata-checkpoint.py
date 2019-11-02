import pyspark
from pyspark import SparkConf,SparkContext
import re
import sys

def main():
    
    #Defining the configuration of the applcation
    conf = SparkConf()
    conf.set("spark.master","local")
    conf.set("spark.app.name","webdata")

    #creation of the spark context object
    sc  =SparkContext(conf=conf)
    
    if (len(sys.argv)==3):        
        #REad the data
        print("Reading the data..")
        webdata = sc.textFile(sys.argv[1])

        #processing
        print("Processing...")
        webdata2 = webdata.map(lambda x:x+"spark")

        #Save the data
        print("Saving the data.....")
        webdata2.saveAsTextFile(sys.argv[2])

        sc.stop()
    else:
        print("Not enoough parametres..")
        exit(1)

if __name__ == '__main__':
    main()
