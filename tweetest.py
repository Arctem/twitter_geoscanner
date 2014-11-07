#!/usr/bin/env python2

import tweepy
from tweepy import StreamListener
import pprint, json
import time

DEBUG = False

consumer_key = open('cons_key.txt', 'r').read().strip()
consumer_secret = open('cons_secret.txt', 'r').read().strip()
access_token_key = open('access_key.txt', 'r').read().strip()
access_token_secret = open('access_secret.txt', 'r').read().strip()

def parse_geo(geo):
  if not geo:
    return None
  if geo['type'] != 'Point':
    print('Error:', geo)
  geo = geo['coordinates']
  ns = str(geo[0]) + ' ' + 'N' if geo[0] >= 0 else 'S'
  ew = str(geo[1]) + ' ' + 'E' if geo[0] >= 0 else 'W'
  return ns + ' ' + ew

class DataStreamer(StreamListener):
  def __init__(self, api = None, fprefix = 'streamer'):
    self.api = api or API()
    self.counter = 0
    self.fprefix = fprefix
    self.start = time.clock()

  def on_status(self, data):
    #print(dir(data))
    if data.geo:
      self.counter += 1
      #print(self.counter)
      if self.counter % 100 == 0:
        print('{} - {} per second'.format(self.counter, self.counter / (time.clock() - self.start) / 60))
    
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
    print('Limit: ' + str(track))
    return

  def on_error(self, status):
    print('Error: ' + str(status))

def main():
  auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token_key, access_token_secret)

  api = tweepy.API(auth)
  l = DataStreamer(api)
  stream = tweepy.Stream(auth, l)
  stream.filter(locations=[-180,-90,180,90], stall_warnings=True)
  #stream.filter(languages=['english'])

if __name__ == '__main__':
  main()