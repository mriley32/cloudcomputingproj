import json
import boto3

#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Load credentials and config
config = json.loads("config.json")

AWS_KEY = config['AWS']['AWS_KEY']
AWS_SECRET = config['AWS']['AWS_SECRET']
REGION = config['AWS']['REGION']

sqs = boto3.resource('sqs', aws_access_key_id=AWS_KEY,
                            aws_secret_access_key=AWS_SECRET,
                            region_name=REGION)
                            
# Get the queue
queue = sqs.get_queue_by_name(QueueName='Pending_Tweets')

#Twitter API Keys
access_token = config['tweepy']['access_token']
access_token_secret = config['tweepy']['access_token_secret']
consumer_key = config['tweepy']['consumer_key']
consumer_secret = config['tweepy']['consumer_secret']

#Implement this function to publish data to SQS
def publish():
    return True    
    
    
#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print json.loads(data)['text']
        queue.send_message(MessageBody=json.dumps(data))
        return True

    def on_error(self, status):
        print status

def main():
    print 'Listening...'
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords
    stream.filter(track=['spacex'])

if __name__ == '__main__':
    main()    