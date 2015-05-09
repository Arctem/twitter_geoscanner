import pprint, json
import time
import re
from datetime import datetime
import sys

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

engine = create_engine('sqlite:///twitter.db')
Base = declarative_base()
Session = sessionmaker()

consumer_key = open('cons_key.txt', 'r').read().strip()
consumer_secret = open('cons_secret.txt', 'r').read().strip()
access_token_key = open('access_key.txt', 'r').read().strip()
access_token_secret = open('access_secret.txt', 'r').read().strip()

rehash = re.compile('(?<!\w)#\w+')

classifier = nltk.data.load('classifiers/movie_reviews_NaiveBayes.pickle')

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
    self.fprefix = fprefix
    self.start = time.clock()

  def on_status(self, data):
    text = dict([(word, True) for word in data.text.split()])
    classification = classifier.classify(text)
    print('{}: {}'.format(classification, repr(data.text)))

    if self.counter == 0:
      print('DataStreamer Starting')
    self.counter += 1

    if self.counter % 10 == 0:
      print('{} - {} tweets per second'.format(self.counter,
        self.counter / (time.clock() - self.start) / 60))

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
      h = Hash(tag = hashtag, tweet = tweet)
      session.add(h)

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
  stream.filter(locations=[-180,-90,180,90], languages=['english'],
    stall_warnings=True)

if __name__ == '__main__':
  main()
