f# Import libraries
import json
import time
import tweepy
import logging
from confluent_kafka import Producer

# Define logging config
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

# Create logging object
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create producer object
p = Producer({'bootstrap.servers':'localhost:9092'})
print('Kafka Producer has been initiated...')

# Define Twitter API credentials
bearer_token = "xxx"

# Define search term
search_term = 'argentina'

# Define a function for Twitter API v2 auth 
def TwitterAPIv2(bearer_token, search_term):
    client = tweepy.Client(bearer_token)
    tweets = client.search_recent_tweets(query=search_term,
                                         max_results=50,
                                         tweet_fields = ['author_id','created_at','text','lang'])
    return tweets

# Define callback
def receipt(err, msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)
        
# Define app
def main():
    for tweet in tweets.data:
        response = {
           'tweet_id': tweet.id,
           'author_id': tweet.data['author_id'],
           'text': tweet.text,
           'created_at': tweet.data['created_at'],
           'lang': tweet.data['lang']    
           }

        m = json.dumps(response)
        p.poll(1)
        
        # publish Kafka topic
        p.produce('twitterapixkafka', m.encode('utf-8'), callback=receipt)
        p.flush()
        time.sleep(3)
        
if __name__ == '__main__':
    tweets = TwitterAPIv2(bearer_token, search_term)
    main()