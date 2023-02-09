'''
Created on 1 Feb 2023

@author: ariel
'''
import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_twitter_etl():
    api_key = "LO3Z2mVGz8srnc1kpPuepelAR"
    api_secret = "Fjjtd1CsClo20Lia2sEuaxpPmscEMaBHe2AmCZ3gGPyRR9WO0z"
    bearer_token = r"AAAAAAAAAAAAAAAAAAAAACNblgEAAAAAC%2B1BmdzIdj2MD2SYulxbZuElThM%3Dj3fK6d7E3uTlQ0w9wBSD0WrwV4604FcBaNge95Fnxbobion9Tr"
    access_token = "1620904570754060288-4h8qQuFrArPvNqaMPrgo0pWubDuRfx"
    access_token_secret = "A0lxWhhyRyJZwTaVANJAC2Ph5dXfhwLtGkM0ddr7i6N6q"
    
    '''Twitter authentication'''
    client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_token_secret)
    
    auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
    
    
    '''Creating an API object'''
    api = tweepy.API(auth)
    
    tweets = api.user_timeline(screen_name='@elonmusk',
                               # 200 is the maximum allowed count
                               count=200,
                               include_rts = False,
                               #Necessary to keep full_text
                               #otherwise only the first 140 words are 
                               tweet_mode = 'extended'
                               )
    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]
        
        refined_tweet = {"user": tweet.user.screen_name,
                         'text': text,
                         'favorite_count': tweet.favorite_count,
                         'retweet_count': tweet.retweet_count,
                         'created_at': tweet.created_at
                         }
        tweet_list.append(refined_tweet)
        
        
    df = pd.DataFrame(tweet_list)
    df.to_csv("s3://sankai-etl-bucket/elonmusk_twitter_data.csv")