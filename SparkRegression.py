from __future__ import print_function


from pyspark.conf import SparkConf
from pyspark.ml.regression import LinearRegression

import pyspark.sql
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
import boto3
import json
import time
import datetime


AWS_KEY= config['AWS']['AWS_KEY']
AWS_SECRET= config['AWS']['AWS_SECRET']
REGION=config['AWS']['REGION']
BUCKET = config['S3']['Bucket']

s3 = boto3.client('s3',aws_access_key_id=AWS_KEY,aws_secret_access_key=AWS_SECRET)
conf = SparkConf().set("spark.jars","mysql-connector-java-5.1.47.jar")
spark = SparkSession\
    .builder\
    .appName("LinearRegressionWithElasticNet")\
    .getOrCreate()
        
sqlContext = SQLContext(spark)
   
url="jdbc:mysql://ece4813-design-project.cehibzqjgc4u.us-east-2.rds.amazonaws.com/ECE4813_Design_Project"
user = "root"
password = "ZZ6uc^hd9!Hw"
dftweets = sqlContext.read.format("jdbc").options(url=url,driver = "com.mysql.jdbc.Driver",dbtable="tweets",user=user,password=password).load()
dfstock = sqlContext.read.format("jdbc").options(url=url,driver = "com.mysql.jdbc.Driver",dbtable="financial_data",user=user,password=password).load()

tweets = dftweets.drop('tweet_text').drop('id').withColumn('date',date_trunc('day',col('date_time'))).drop('date_time')
aggtweets = tweets.groupBy('date','twitter_handle').agg(avg(tweets.sentiment_score).alias('sentiment_score'))
sent_sums = aggtweets.groupBy('date').pivot('twitter_handle').sum('sentiment_score').orderBy('date')
sent_sums.show()
dfstock = dfstock.sort("date_time",ascending=True)
groupedstock = dfstock.groupby("ticker_code")
dummy = dfstock.dropDuplicates(['ticker_symbol'])
listofcomp = dummy.select("ticker_symbol").collect()
dummytweetdf = dftweets.dropDuplicates(['twitter_handle'])
rowsofhandles = dummytweetdf.select("twitter_handle").collect()
#print(rowsofhandles)
handles = []
for j in rowsofhandles:
    handles.append(j.twitter_handle)
print(handles)
outputdict = {}

for i in listofcomp:
    my_window = Window.partitionBy().orderBy('date_time')
    mydf = dfstock.filter(dfstock.ticker_symbol == i.ticker_symbol)
    mydf = mydf.withColumn("prev_stock",F.lag(mydf.stock_price).over(my_window))
    mydf = mydf.withColumn("diff",F.when(F.isnull(mydf.stock_price - mydf.prev_stock),0).otherwise(mydf.stock_price - mydf.prev_stock))
    newdf = mydf.drop('id','ticker_symbol','industry','stock_price','prev_stock').join(sent_sums,mydf.date_time == sent_sums.date)
    #newdf.show()
    newdf = newdf.drop('date_time').na.fill(0.0)
    #newdf.show()
    vectorAssembler = VectorAssembler(inputCols = handles,outputCol = 'features')
    lregdf = vectorAssembler.transform(newdf)
    #lregdf.show(20,truncate=100)
    lregdf = lregdf.select(['features','diff'])
    #lregdf.show(4,truncate=150)
    lr = LinearRegression(featuresCol = 'features', labelCol = 'diff',maxIter=10, regParam=0.3, elasticNetParam=0.8)
    lrModel = lr.fit(lregdf)
    print(i.ticker_symbol)
    print("R2: "+str(lrModel.summary.r2))
    print("Coefficients: " + str(lrModel.coefficients))
    print("Intercept: " + str(lrModel.intercept))
    r2 = lrModel.summary.r2
    coef = {}
    var = 0
    for x in handles:
        coef[x] = lrModel.coefficients[var]
        var = var + 1
    mydict = {'R2':r2,'coef':coef}
    outputdict[i.ticker_symbol] = mydict

ts=time.time()
timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
outputname = str(timestamp) + ".txt"
s3.put_object(Bucket=BUCKET,Key = outputname,Body=json.dumps(outputdict))

