import json

#Import the necessary methods from tweepy library
import tweepy
from tweepy.streaming import StreamListener

#Load credentials and config
json_data=open("config.json").read()
config = json.loads(json_data)

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
twitter_handles = json.loads(json_data)
for i in twitter_handles['handles']:
    user = twitter_api.get_user(i)
    user_ids.append(user.id)

#Listener that prints tweets and adds them to a sqs queue
class StdOutListener(StreamListener):

    def on_data(self, data):
        print json.loads(data)
        exit()
        return True

    def on_error(self, status):
        print status

def main():
    print 'Listening...'
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    
    stream = tweepy.Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords
    stream.filter(track=twitter_handles['handles'][0])

if __name__ == '__main__':
    main()    