# This is a spark app. to illustartate wordcount 


from pyspark import SparkConf,SparkContext
import sys
import configparser
from configparser import ConfigParser

conf = SparkConf()
sc = SparkContext(conf=conf)

config = configparser.ConfigParser()

print(config)
config.read('/home/ec2-user/project1/resources/config.ini')
input = config.get('Dev','input')
output = config.get('Dev','output')

print('Reading the data from the input file')
try:
    input = sc.textFile(input)
except Exception:
    print('Please pass the input path from config file...')
    exit(1)

print('started transformation..')
wc = input.flatMap(lambda x:x.split(' ')).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
wc.collect()

print('saving the file..')
try:
    wc.saveAsTextFile(output)
except Exception:
    print("Please pass the output path from config file...")
    exit(1)

#Stops the application 
sc.stop()


