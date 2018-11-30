import sys
import re
import json
import pymysql
from datetime import datetime, date, time, timedelta
import dateutil.parser as parser
import multiprocessing as mp

#Import the necessary methods from tweepy library
import tweepy
from tweepy.streaming import StreamListener

#Load credentials and config
json_data=open("config.json").read()
config = json.loads(json_data)

#DB Credentials
USERNAME = config['RDS']['USERNAME']
PASSWORD = config['RDS']['PASSWORD']
DB_NAME = config['RDS']['DB_NAME']
HOST = config['RDS']['HOST']

#Twitter API Keys
access_token = config['tweepy']['access_token']
access_token_secret = config['tweepy']['access_token_secret']
consumer_key = config['tweepy']['consumer_key']
consumer_secret = config['tweepy']['consumer_secret']

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

twitter_api = tweepy.API(auth)

#Load targeted twitter handles
user_ids = []
json_data=open('twitter_handles.json').read()
twitter_handles = json.loads(json_data)["handles"]
for i in twitter_handles:
    user = twitter_api.get_user(i)
    user_ids.append(str(user.id))
print user_ids

#Read parameters to set start and stop time
if len(sys.argv) == 2:
    yesterday = datetime.strptime(sys.argv[1],'%Y-%m-%d').date()
    tweets_begin = datetime.combine(yesterday, time.min)
    tweets_end = datetime.combine(yesterday, time.max)  
elif len(sys.argv) > 2:
    yesterday = datetime.strptime(sys.argv[1],'%Y-%m-%d').date()
    tweets_begin = datetime.combine(yesterday, time.min)
    tweets_end = datetime.combine(datetime.strptime(sys.argv[2],'%Y-%m-%d').date(), time.max)
else:
    tweets_begin = datetime.combine(date.today() - timedelta(days=1), time.min)
    tweets_end = datetime.combine(date.today() - timedelta(days=1), time.max)

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

def produceTweets(id):
    conn = pymysql.connect (host = HOST,
                user = USERNAME,
                passwd = PASSWORD,
                db = DB_NAME, 
		        port = 3306, charset='utf8mb4', use_unicode=True)
    
    cursor = conn.cursor()

    items = twitter_api.user_timeline(user_id = id, tweet_mode="extended", wait_on_rate_limit=True)
    while(True):
        if len(items) < 1 or items[0].created_at <= tweets_begin:
            break

        if items[-1].created_at <= tweets_end:
            for i in items:
                if tweets_begin <= i.created_at <= tweets_end:
                    #print json.dumps(i._json)
                    #print "@%s (%s) '%s'" % (i.user.screen_name, str(i.created_at), i.full_text)
                    
                    data = json.loads(json.dumps(i._json))

                    handle = data['user']['screen_name']
                    tweet = data['full_text']
                    date_time = parser.parse(data['created_at'])
        
                    sentiment = findsentiment(data)
                    
                    statement = "SELECT * FROM tweets WHERE twitter_handle='%s' AND date_time='%s';" % (handle.replace("'","''"), date_time.strftime(timestamp_f))
                    print statement
                    cursor.execute(statement)
                    
                    if cursor.fetchone() == None:
                        statement = "INSERT INTO tweets (twitter_handle, date_time, tweet_text, sentiment_score) VALUES ('%s', '%s', '%s', %f);" % (handle.replace("'","''"), date_time.strftime(timestamp_f),tweet.replace("'","''"), sentiment)
                        print statement
                        cursor.execute(statement)
        
        items = twitter_api.user_timeline(user_id = id, tweet_mode="extended", max_id = items[-1].id, wait_on_rate_limit=True)
    
    conn.commit()
    cursor.close()
    conn.close() 

#Setup multiprocessing pool
tweet_pool = mp.Pool(processes=20)

def main():
    #produceTweets(user_ids[5])
    tweet_pool.map_async(produceTweets, user_ids).get(9999999)

    tweet_pool.close()
    tweet_pool.join()

if __name__ == '__main__':
    main()