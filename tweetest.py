#!/usr/bin/env python2

import tweepy
from tweepy import StreamListener
import pprint, json
import time
import re
import db
from datetime import datetime

DEBUG = False

consumer_key = open('cons_key.txt', 'r').read().strip()
consumer_secret = open('cons_secret.txt', 'r').read().strip()
access_token_key = open('access_key.txt', 'r').read().strip()
access_token_secret = open('access_secret.txt', 'r').read().strip()

rehash = re.compile('(?<!\w)#\w+')

EXITAFTERTHREEMINUTES = False
init_time = datetime.now()

def parse_geo(geo):
  if not geo:
    return None
  if geo['type'] != 'Point':
    print('Geo Error:', geo)
  geo = geo['coordinates']
  ns = str(geo[0]) + ' ' + 'N' if geo[0] >= 0 else 'S'
  ew = str(geo[1]) + ' ' + 'E' if geo[0] >= 0 else 'W'
  return ns + ' ' + ew

class DataStreamer(StreamListener):
  def __init__(self, api = None, fprefix = 'streamer', conn = None):
    self.api = api or API()
    self.counter = 0
    self.hash_counter = 0
    self.fprefix = fprefix
    self.start = time.clock()
    self.hashes = {}
    self.conn = conn

  def on_status(self, data):
    if EXITAFTERTHREEMINUTES:
      if (datetime.now() - init_time).total_seconds() > (60*3):
        quit()
    
    #print(dir(data))
    if data.geo:
      if self.counter == 0:
        print('DataStreamer Starting')
      self.counter += 1
      #print(self.counter)
      if self.counter % 1000 == 0:
        print('{} - {} tweets per second'.format(self.counter, self.counter / (time.clock() - self.start) / 60))
      if '#' in data.text:
        self.hash_counter += 1
        if self.hash_counter % 100 == 0:
          print('{} - {} hashed tweets per second'.format(self.hash_counter, self.hash_counter / (time.clock() - self.start) / 60))

        hashtags = rehash.findall(data.text)
        hashtags_list = []
        for hashtag in hashtags:
          hashtag = hashtag.lstrip("#")
          if len(hashtag) > 0:
            hashtags_list.append(hashtag)

        if len(hashtags_list) > 0 and self.conn:
          for hashtag in hashtags_list:
            hash_occurances = self.conn.increaseHashtagCount(hashtag)
            if hash_occurances % 10 == 0:
              print('{} has occured {} times.'.format(hashtag, hash_occurances))
          geo = data.geo["coordinates"]
          self.conn.insertTweet({"geo": {"lat":geo[0],"lon":geo[1]}, "tags": hashtags_list})
    
    if DEBUG:
      print("=" * 40)
      print(data.author.screen_name + ': ' + data.author.name)
      #print(dir(data.author))
      print(data.text)
      #print(data.geo, data.coordinates)
      print(u'{}, located at {}, said "{}".'.format(data.author.name,
        parse_geo(data.geo), data.text))
      #return not data.geo or not data.coordinates

    return True

  def on_limit(self, track):
    print('DataStreamer Limit: ' + str(track))
    return

  def on_error(self, status):
    print('DataStreamer Error: ' + str(status))

def main():
  auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token_key, access_token_secret)

  connection = db.Connection()

  api = tweepy.API(auth)
  l = DataStreamer(api, conn=connection)
  stream = tweepy.Stream(auth, l)
  stream.filter(locations=[-180,-90,180,90], stall_warnings=True)
  #stream.filter(languages=['english'])

if __name__ == '__main__':
  main()
