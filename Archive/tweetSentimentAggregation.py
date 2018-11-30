from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, date_trunc, avg

conf = SparkConf().set("spark.jars", "mysql-connector-java-5.1.47.jar")
spark = SparkSession \
    .builder.config(conf=conf) \
    .appName("Tweet Sentiment Analysis") \
    .getOrCreate()

sqlContext = SQLContext(spark)

user = "root"
password="ZZ6uc^hd9!Hw"
url="jdbc:mysql://ece4813-design-project.cehibzqjgc4u.us-east-2.rds.amazonaws.com/ECE4813_Design_Project"

tweets = sqlContext.read.format("jdbc").options(url = url, driver = "com.mysql.jdbc.Driver",dbtable = "tweets", user=user, password=password).load()

tweets = tweets.drop('tweet_text', 'id').withColumn('date', date_trunc('day',col('date_time'))).drop('date_time')

aggregatedTweets = tweets.groupBy('date', 'twitter_handle').agg(avg(tweets.sentiment_score).alias('sentiment_score'))

aggregatedTweets.groupBy('date').pivot('twitter_handle').sum('sentiment_score')