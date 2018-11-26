import sys
import json
import boto3
from datetime import datetime, date, time, timedelta
import dateutil.parser as parser
import multiprocessing as mp

#Import the necessary methods from tweepy library
import tweepy
from tweepy.streaming import StreamListener

#Load credentials and config
json_data=open("config.json").read()
config = json.loads(json_data)

AWS_KEY = config['AWS']['AWS_KEY']
AWS_SECRET = config['AWS']['AWS_SECRET']
REGION = config['AWS']['REGION']

sqs = boto3.resource('sqs', aws_access_key_id=AWS_KEY,
                            aws_secret_access_key=AWS_SECRET,
                            region_name=REGION)

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

def produceTweets(id):
    # Get the queue
    queue = sqs.get_queue_by_name(QueueName='Pending_Tweets')

    items = twitter_api.user_timeline(user_id = id, tweet_mode="extended")
    while(True):
        if len(items) < 1 or items[0].created_at <= tweets_begin:
            break

        if items[-1].created_at <= tweets_end:
            for i in items:
                if tweets_begin <= i.created_at <= tweets_end:
                    #print json.dumps(i._json)
                    print "@%s (%s) '%s'" % (i.user.screen_name, str(i.created_at), i.full_text)
                    queue.send_message(MessageBody=json.dumps(i._json))
        
        items = twitter_api.user_timeline(user_id = id, tweet_mode="extended", max_id = items[-1].id)

#Setup multiprocessing pool
tweet_pool = mp.Pool(processes=10)

def main():
    produceTweets(user_ids[5])
    #tweet_pool.map_async(produceTweets, user_ids).get(9999999)

    #tweet_pool.close()
    #tweet_pool.join()

if __name__ == '__main__':
    main()