#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import findspark
findspark.init("/home/ubuntu/cloudcomputingproj/spark-2.0.0-bin-hadoop2.7")
# $example on$
from pyspark.conf import SparkConf
from pyspark.ml.regression import LinearRegression
# $example off$
import pyspark.sql
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


if __name__ == "__main__":
    
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
    aggtweets.groupBy('date').pivot('twitter_handle').sum('sentiment_score')
    dfstock = dfstock.sort("date_time",ascending=True)
    groupedstock = dfstock.groupby("ticker_code")
    dummy = dfstock.dropDuplicates(['ticker_symbol'])
    listofcomp = dummy.select("ticker_symbol").collect()
    for i in listofcomp:
        print(i.ticker_symbol)
    #print(listofcomp)
    #groupedstock.show()
    #properties = {"driver":"mysql-connector-java-5.0.8-bin.jar"}
    #df = sqlContext.read.jdbc(url=url,table="tweets", **properties)
    print("HERE!")
    # $example on$
    # Load training data
    training = spark.read.format("libsvm")\
        .load("../../../../../data/mllib/sample_linear_regression_data.txt")

    lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Fit the model
    lrModel = lr.fit(training)

    # Print the coefficients and intercept for linear regression
    print("Coefficients: " + str(lrModel.coefficients))
    print("Intercept: " + str(lrModel.intercept))
    # $example off$

    spark.stop()
