#Packages
import pyspark
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
import sys
import re
#--------------------------------------------------------------
#Funtion to extract the required fields
## added some comment on this code
def extract_calldata(rec):
    phonenos = re.search('\d{20}',rec).group()
    from_no = phonenos[0:10]
    to_no = phonenos[10:20]
    status = re.search('DROPPED|FAILED|SUCCESS',rec).group()
    timestamps = re.findall('\d{4}-\d{2}-\d{2}\s{1}\d{2}:\d{2}:\d{2}',rec)
    start_time = timestamps[0]
    end_time = timestamps[1]
    return (from_no,to_no,status,start_time,end_time)

def main():
    
    #Creation of contexts
    conf = SparkConf()
    sc = SparkContext(conf=conf) #Spark context object
    sqlc = SQLContext(sc)  #SQLContext object to deal with dataframes

    if(len(sys.argv)!=4):
        print("Error passing arguments..pass 3 arguments")
        exit(1)
    else:
        #Reading input data
        print('Reading the input....')
        r1 = sc.textFile(sys.argv[1])

        #Transformation 
        print('Tranforming the data...')
        r2 = r1.map(lambda rec: extract_calldata(rec))

        #Save the result 
        print('Saving the data..')
        r2.saveAsTextFile(sys.argv[2])

        #Creation of dataframe from RDD
        df = r2.toDF(['from_no','to_no','status','start_time','end_time']) 
        print(df.show(10))
       # df.write.format("csv").save(sys.argv[3])
    #Stop the application
    sc.stop()

if __name__ == '__main__':
    main()


