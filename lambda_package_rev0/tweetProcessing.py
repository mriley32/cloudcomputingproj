import re
import json
import pymysql
from datetime import datetime, date, time, timedelta
import dateutil.parser as parser

#Load credentials and config
json_data=open("config.json").read()
config = json.loads(json_data)

#DB Credentials
USERNAME = config['RDS']['USERNAME']
PASSWORD = config['RDS']['PASSWORD']
DB_NAME = config['RDS']['DB_NAME']
HOST = config['RDS']['HOST']

TERMS={}

#-------- Load Sentiments Dict ----
sent_file = open('AFINN-111.txt')
sent_lines = sent_file.readlines()
for line in sent_lines:
	s = line.split("\t")
	TERMS[s[0]] = s[1]

sent_file.close()

#-------- Find Sentiment  ----------
def findsentiment(tweet):
    sentiment=0.0

    if tweet.has_key('full_text'):
        text = tweet['full_text']		
        text=re.sub('[!@#$)(*<>=+/:;&^%#|\{},.?~`]', '', text)
        splitTweet=text.split()

        for word in splitTweet:
            if TERMS.has_key(word):
                sentiment = sentiment+ float(TERMS[word])

    return sentiment

timestamp_f = '%Y-%m-%d %H:%M:%S'

def lambda_handler(event, context):
    conn = pymysql.connect (host = HOST,
                user = USERNAME,
                passwd = PASSWORD,
                db = DB_NAME, 
		        port = 3306, charset='utf8mb4', use_unicode=True)
    
    cursor = conn.cursor()
    
    for record in event['Records']:
        data = json.loads(record['body'])
        
        handle = data['user']['screen_name']
        tweet = data['full_text']
        date_time = parser.parse(data['created_at'])
        
        sentiment = findsentiment(data)

        statement = 'INSERT INTO tweets (twitter_handle, date_time, tweet_text, sentiment_score) VALUES ("%s", "%s", "%s", %f)' % (handle, date_time.strftime(timestamp_f),tweet, sentiment)
        cursor.execute(statement)


    conn.commit()
    cursor.close()
    conn.close()

    return ''



