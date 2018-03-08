import os
import shutil
import re
import sys
import time
from pyspark import SparkConf, SparkContext
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars postgresql-42.2.1.jar --driver-class-path postgresql-42.2.1.jar'
conf = SparkConf()
sc = SparkContext(conf=conf)

import shutil
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, FloatType, DateType,TimestampType
sqlContext = SQLContext(sc)
filename = sys.argv[1]
date=filename[-8:]
#process data 

raw_data=sc.textFile(filename)
header = raw_data.first()
raw_data=raw_data.filter(lambda line: line != header)
data=raw_data.map(lambda a: (datetime.strptime(date+a[0:9], "%Y%m%d%H%M%S%f"),a[9],a[10:16],float(a[26:37])/10000,int(a[37:44]),float(a[44:55])/10000,int(a[55:62]),a[62],a[63:67],a[67],a[68],a[69:85],a[85],a[86],a[87],a[88]))
schema = StructType([StructField("date", TimestampType(), False),StructField("exchange", StringType(), False),StructField("symbol",StringType(),False),StructField("bid price",FloatType(),False),StructField("bid size",IntegerType(),False),StructField("ask price",FloatType(),False),StructField("ask size",IntegerType(),False),StructField("Quote Condition",StringType(),False),StructField("market maker",StringType(),False),StructField("bid exchange",StringType(),False),StructField("ask exchange",StringType(),False),StructField("sequence number",StringType(),False),StructField("National BBO Ind",StringType(),False),StructField("NASDAQ BBO Ind",StringType(),False),StructField("Quote Cancel/Correction",StringType(),False),StructField("Source of quote",StringType(),False)])
data_df = sqlContext.createDataFrame(data, schema)
#Connect database

url_pyspark="jdbc:postgresql://rds-postgresql.clahixkkwlcw.us-east-1.rds.amazonaws.com:5432/stockdata?user=administrator&password=cornellorie"
data_df.write.jdbc(url_pyspark,"quote_"+date,mode="overwrite")
