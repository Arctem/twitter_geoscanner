#!/usr/bin/env python2

import tweepy
from tweepy import StreamListener
import pprint, json
import time
import re
import db
from datetime import datetime
import numpy

connection  = db.Connection()

class Analysis:
  def __init__(self):
    pass

  # We want to score tweets based on the following formula:
  # tagTrendScore(hashtag, hour, day, week) =
  #   |hashtagOccurances(hashtag, hour, dayOfWeek, week)| -
  #   ( occurancesMean(hashtag, hour, dayOfWeek) / 
  #     occurancesStandardDeviation(hashtag, hour, dayOfWeek) )
  def tagTrendScore(self, hashtag, hour, dayOfWeek, week):
    numOccurances = self.hashtagOccurances(hashtag, hour, dayOfWeek, week)
    occurancesMean = self.occurancesMean(hashtag, hour, dayOfWeek)
    occurancesStandardDeviation = self.occurancesStandardDeviation(hashtag, hour, dayOfWeek)
    return numOccurances - (occurancesMean / occurancesStandardDeviation)

  # @return the number of tags that occur within the given time period that contain
  #   the given hashtag, or -1 if the time parameters are out of bounds
  def hashtagOccurances(self, hashtag, hour, dayOfWeek, week):
    time = connection.getDBTime(hour, dayOfWeek, week)
    if (time < 0):
      return -1
    sph = 60 * 60
    return connection.countTweets(time, time+sph, [hashtag])

  # return A list of counts of hashtag occurances for all weeks of the given day/hour
  def allHashtagOccurances(self, hashtag, hour, dayOfWeek):
    greatestWeek = connection.getGreatestWeek(hour, dayOfWeek)
    counts = []
    for week in range(greatestWeek):
      counts.append(self.hashtagOccurances(hashtag, hour, dayOfWeek, week))
    return counts

  # @return The mean of the number of occurances over all weeks
  def occurancesMean(self, hashtag, hour, dayOfWeek):
    counts = self.allHashtagOccurances(hashtag, hour, dayOfWeek)
    arr = numpy.array(counts)
    return numpy.mean(arr)

  # @return the standard deviation of the number of occurances over all weeks
  def occurancesStandardDeviation(self, hashtag, hour, dayOfWeek):
    counts = self.allHashtagOccurances(hashtag, hour, dayOfWeek)
    arr = numpy.array(counts)
    return numpy.std(arr)

def main():
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