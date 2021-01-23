#I want to write a Spark Streamin App which takes a stream with random integers and count them. Here is the Spark app I wrote:
import socket
from random import randint
import requests
import requests_oauthlib
import json

# Include your Twitter account details
ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', 'iphone')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            tweet = full_tweet['text'] + "\n"
            tcp_connection.send(bytes(tweet, 'utf-8'))

        except:
            continue


host = 'localhost'
port = 8100
address = (host, port)

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(address)
server_socket.listen()


print("Listening for client . . .")
conn, address = server_socket.accept()
print("Connected to client at ", address)

while True:
    resp = get_tweets()
    send_tweets_to_spark(resp, conn)

