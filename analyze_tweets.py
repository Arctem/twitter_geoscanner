#!/usr/bin/env python2

import tweepy
from tweepy import StreamListener
import pprint, json
import time
import re
import db
from datetime import datetime

def main():
  connection  = db.Connection()
  first_tweet = "SELECT * FROM `tweets` ORDER BY `time` ASC LIMIT 1"
  last_tweet = "SELECT * FROM `tweets` ORDER BY `time` DESC LIMIT 1"

  ten_minutes = 60 * 10

  connection.sqlCall(first_tweet, {})
  first_time = [x for x in connection.cursor][0][0]
  start_time = first_time / ten_minutes * ten_minutes
  print('Start time: {}'.format(start_time))

  connection.sqlCall(last_tweet, {})
  last_time = [x for x in connection.cursor][0][0]
  end_time = (last_time / ten_minutes + 1) * ten_minutes
  print('End time: {}'.format(end_time))

  count_range = "SELECT COUNT(*) FROM `tweets` WHERE (`time`>%(start)s AND `time`<%(end)s);"
  results = []
  for t in range(start_time, end_time, ten_minutes):
    connection.sqlCall(count_range, {'start': t, 'end': t + ten_minutes})
    results.append([x for x in connection.cursor][0][0])
    print(t, results[-1])
  print(results) #number of tweets per 10 minutes


if __name__ == '__main__':
  main()