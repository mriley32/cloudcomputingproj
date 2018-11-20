import re
import json
import MySQLdb
import boto3

import StockData

#Load credentials and config
config = json.loads("config.json")

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

    if tweet.has_key('text'):
        text = tweet['text']		
        text=re.sub('[!@#$)(*<>=+/:;&^%#|\{},.?~`]', '', text)
        splitTweet=text.split()

        for word in splitTweet:
            if TERMS.has_key(word):
                sentiment = sentiment+ float(TERMS[word])

    return sentiment

def getFinancialData(timestamp):
    #add code to get financial data
    return 0


def lambda_handler(event, context):
    conn = MySQLdb.connect (host = HOST,
                user = USERNAME,
                passwd = PASSWORD,
                db = DB_NAME, 
		        port = 3306)
    
    cursor = conn.cursor()
    
    # TODO implement
    for record in event:
        data = json.loads(record['body'])
        handle = ''
        tweet = data['text']
        date_time = ''
        sentiment = findsentiment(data)

        cursor.execute('INSERT INTO tweets (twitter_handle, date_time, tweet_text, sentiment_score) VALUES ("%s", %s, "%s", %f)' % (handle, date_time, tweet, sentiment))


    conn.commit()
    cursor.close()
    conn.close() 



