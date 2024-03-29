#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark Streaming Individual Assignment 
Anggie Legiando Pratama
IE HST MBS O2 2018/2019

@author: legiando
"""

import socket
import sys
import requests
import requests_oauthlib
import json

ACCESS_TOKEN = '3106812974-drNpGWMkS3wT5JPCoj3Vbk7ZWHo5fN4KHOFma6F'
ACCESS_SECRET = 'tpXPfEd0aNaL6WVxeJkt7xfEEA6DgXxWsv5d2KSGfYsQh'
CONSUMER_KEY = '9sKTPHAp31aYBIijFTeFcbvrP'
CONSUMER_SECRET = 'kMUsnNdgsPoqdSbyZs3adz3iUw6MoRtmsc4go0xnUgkKZSWtDs'

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    # Bounding box for Madrid location '-3.7834,40.3735,-3.6233,40.4702'
    query_data = [('language', 'es'), ('locations', '-3.7834,40.3735,-3.6233,40.4702'),('track','Madrid')]
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
            print ("------------------------------------------")
            tcp_connection.send(bytes(tweet_text + '\n', "utf-8"))
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)
         
TCP_IP = "localhost"
TCP_PORT = 6666
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)