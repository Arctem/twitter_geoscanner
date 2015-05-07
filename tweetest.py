#!/usr/bin/env python2

import pprint, json
import time
import re
from datetime import datetime
import sys
import ConfigParser

import tweepy
from tweepy import StreamListener
import nltk.data
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship, backref
from sqlalchemy import ForeignKey
from sqlalchemy import Sequence

conf = ConfigParser.RawConfigParser()
conf.read('mysql.conf')
user = conf.get('db', 'user')
password = conf.get('db', 'password')
host = conf.get('db', 'host')
database = conf.get('db', 'database')

#engine = create_engine('sqlite:///:memory:', echo=True)
#engine = create_engine('mysql://{user}:{password}@{host}/{database}'.format(user=user, password=password, host=host, database=database))
engine = create_engine('sqlite:///twitter.db')
Base = declarative_base()
Session = sessionmaker()

DEBUG = False

consumer_key = open('cons_key.txt', 'r').read().strip()
consumer_secret = open('cons_secret.txt', 'r').read().strip()
access_token_key = open('access_key.txt', 'r').read().strip()
access_token_secret = open('access_secret.txt', 'r').read().strip()

rehash = re.compile('(?<!\w)#\w+')

classifier = nltk.data.load('classifiers/movie_reviews_NaiveBayes.pickle')

EXITAFTERTHREEMINUTES = False
init_time = datetime.now()

class Tweet(Base):
  __tablename__ = 'tweets'

  id = Column(Integer, Sequence('tweet_id_sequence'), primary_key=True)
  text = Column(String)
  sentiment = Column(String)
  date = Column(DateTime, default=datetime.utcnow)

class Hash(Base):
  __tablename__ = 'hash'

  id = Column(Integer, Sequence('hash_id_sequence'), primary_key=True)
  tag = Column(String)
  tweet_id = Column(Integer, ForeignKey('tweets.id'))
  tweet = relationship("Tweet", backref=backref('hashes', order_by=id))

class DataStreamer(StreamListener):
  def __init__(self, api = None, fprefix = 'streamer'):
    self.api = api or API()
    self.counter = 0
    self.hash_counter = 0
    self.fprefix = fprefix
    self.start = time.clock()
    self.hashes = {}

  def on_status(self, data):
    if EXITAFTERTHREEMINUTES:
      if (datetime.now() - init_time).total_seconds() > (60*3):
        quit()
    
    text = dict([(word, True) for word in data.text.split()])
    classification = classifier.classify(text)
    print('{}: {}'.format(classification, repr(data.text)))
    
    if self.counter == 0:
      print('DataStreamer Starting')
    self.counter += 1
    #print(self.counter)
    if self.counter % 10 == 0:
      print('{} - {} tweets per second'.format(self.counter, self.counter / (time.clock() - self.start) / 60))

    hashtags = rehash.findall(data.text)
    hashtags_list = []
    for hashtag in hashtags:
      hashtag = hashtag.lstrip("#")
      if len(hashtag) > 0:
        hashtags_list.append(hashtag)

    session = Session()
    tweet = Tweet(text = data.text, sentiment=classification)
    session.add(tweet)
    for hashtag in hashtags_list:
      #hash_occurances = self.conn.increaseHashtagCount(hashtag)
      h = Hash(tag = hashtag, tweet = tweet)
      session.add(h)
      # if hash_occurances % 10 == 0:
      #   print('{} has occured {} times.'.format(hashtag, hash_occurances))
    
    if DEBUG:
      print("=" * 40)
      print(data.author.screen_name + ': ' + data.author.name)
      #print(dir(data.author))
      print(data.text)
      #print(data.geo, data.coordinates)
      print(u'{} said "{}".'.format(data.author.name, data.text))
      #return not data.geo or not data.coordinates
    
    session.commit()
    return True

  def on_limit(self, track):
    print('DataStreamer Limit: ' + str(track))
    return

  def on_error(self, status):
    print('DataStreamer Error: ' + str(status))

def main():
  auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token_key, access_token_secret)

  Base.metadata.create_all(engine)
  Session.configure(bind=engine)

  api = tweepy.API(auth)
  l = DataStreamer(api)
  stream = tweepy.Stream(auth, l)
  stream.filter(locations=[-180,-90,180,90], stall_warnings=True)
  #stream.filter(track=['#obama'], stall_warnings=True)

if __name__ == '__main__':
  main()
