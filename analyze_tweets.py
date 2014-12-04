#!/usr/bin/env python2

import tweepy
from tweepy import StreamListener
import pprint, json
import time
import re
import db
from datetime import datetime, date, timedelta
import numpy



class Analysis:
  def __init__(self):
    self.connection  = db.Connection()
    self.dbtime = db.dbTime(self.connection)

  # Get the tagTrendScore for all hour, days, and weeks
  def tagTrendScoreAllTimes(self, hashtag):
    trendScores = {}
    for hour in range(24):
      for dayOfWeek in range(7):
        greatestWeek = dbtime.getGreatestWeek(hour,dayOfWeek)
        for week in range(greatestWeek):
          dt = dbtime.toRealTime(dbtime.getDBTime(hour, dayOfWeek, week))
          iso = dt.isoformat()
          score = self.tagTrendScore(hashtag, hour, dayOfWeek, week)
          trendScores[iso] = score
          return trendScores

    def getTopHashes(self, limit=100):
      
      connection.sqlCall("select * from hashtag_codes order by count desc limit "+limit+";")
      for row in connection.cursor:
        topHashes.append(row[0:2])
        return topHashes

  def tagTrendScoresTopHashes():
     return tagTrendScores

  def getDataToPlot(hashtag, timePeriod):
    return dataToPlot

        

  # We want to score tweets based on the following formula:
  # tagTrendScore(hashtag, hour, day, week) =
  #   |hashtagOccurances(hashtag, hour, dayOfWeek, week)| -
  #   ( occurancesMean(hashtag, hour, dayOfWeek) / 
  #     occurancesStandardDeviation(hashtag, hour, dayOfWeek) )
  def tagTrendScore(self, hashtag, hour, dayOfWeek, week):
    numOccurances = self.hashtagOccurances(hashtag, hour, dayOfWeek, week)
    occurancesMean = self.occurancesMean(hashtag, hour, dayOfWeek)
    occurancesStandardDeviation = self.occurancesStandardDeviation(hashtag, hour, dayOfWeek)
    if (occurancesStandardDeviation == 0):
      return 0
      return numOccurances - (occurancesMean / occurancesStandardDeviation)

  # @return the number of tags that occur within the given time period that contain
  #   the given hashtag, or -1 if the time parameters are out of bounds
  def hashtagOccurances(self, hashtag, hour, dayOfWeek, week):
    time = dbtime.getDBTime(hour, dayOfWeek, week)
    if (time < 0):
      return -1
      sph = 60 * 60
      return self.connection.countTweets(time, time+sph, [hashtag])

  # return A list of counts of hashtag occurances for all weeks of the given day/hour
  def allHashtagOccurances(self, hashtag, hour, dayOfWeek):
    greatestWeek = dbtime.getGreatestWeek(hour, dayOfWeek)
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
    
  def get_valid_ranges(self):
    first_tweet = "SELECT * FROM `tweets` ORDER BY `time` ASC LIMIT 1"
    last_tweet = "SELECT * FROM `tweets` ORDER BY `time` DESC LIMIT 1"

    ten_minutes = 60 * 10

    self.connection.sqlCall(first_tweet, {})
    first_time = [x for x in self.connection.cursor][0][0]
    start_time = first_time / ten_minutes * ten_minutes
    print('Start time: {}'.format(start_time))

    self.connection.sqlCall(last_tweet, {})
    last_time = [x for x in self.connection.cursor][0][0]
    end_time = (last_time / ten_minutes + 1) * ten_minutes
    print('End time: {}'.format(end_time))

    count_range = "SELECT COUNT(*) FROM `tweets` WHERE (`time`>%(start)s AND `time`<%(end)s);"
    results = []
    start = None
    ranges = []
    for t in range(start_time, end_time, ten_minutes):
      self.connection.sqlCall(count_range, {'start': t, 'end': t + ten_minutes})
      results.append((t, [x for x in self.connection.cursor][0][0]))
      
      if results[-1] != 0:
        if start is None:
          start = t
        else:
          if start is not None:
            ranges.append((start, t, t - start))
            start = None
            print(ranges[-1])
            
    #print(t, results[-1])

    #print(results) #number of tweets per 10 minutes
    #print(ranges)
    return results, ranges

  def make_csv(self, filename):
    results, ranges = self.get_valid_ranges()
    ranges = sorted(ranges, key=lambda r: r[2])
    print(ranges[-10:])

    nov_one = datetime(2014, 11, 1)
    out = open(filename, 'w')
    for r in results:
      time = r[0]
      count = r[1]

      time = nov_one + timedelta(seconds=time)
      print(time)

      out.write('{},{}\n'.format(time, count))
    out.close()
  


def main():
  analysis = Analysis()
  analysis.make_csv('data.csv')


if __name__ == '__main__':
  main()
