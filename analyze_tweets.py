#!/usr/bin/env python2

import tweepy
from tweepy import StreamListener
import pprint, json
import time
import re
import db
from datetime import datetime
import numpy



class Analysis:
  def __init__(self):
    self.connection  = db.Connection()
    self.dbtime = db.dbTime(self.connection)
    self.rank_types_and_versions = {
      "idf": {"full_name": "inverse document frequency",
              "type": 1,
              "version": 1}
    }
    self.topTrendScoreCount = 100

  # If the stored hashtag rank_type, rank_version, and last_tweet_time are all
  #   the same, and there are at least as many stored top hourly rankings as
  #   self.topTrendScoreCount, then the rankings can be loaded instead of having
  #   to be re-computed.
  # @param hashtag The hashtag id
  # @param ranking The name of the ranking (key in self.rank_types_and_versions)
  # @return List of rankings, as pairs of [rank,dbtime], in descending rank order
  #   returns the whole row, instead, if getWholeRow is True
  def _loadPrecomputedTrendScores(self, hashtag, ranking, getWholeRow = False):
    rank_t_v = self.rank_types_and_versions[ranking]
    last_tweet_time = self.dbtime.getLastTime()
    self.connection.sqlCall("SELECT `rank_type`,`rank_version`,`last_tweet_time`,`rankings` FROM `hashtag_codes` WHERE `id`='%(hashtag)s'",
                            {"hashtag":hashtag})
    for row in self.connection.cursor:
      if (row[3] == None or
          row[0] & rank_t_v["type"] == 0 or
          row[1] != rank_t_v["version"] or
          row[2] != last_tweet_time):
        return None
      rankings = json.loads(row[3])[ranking]
      if (len(rankings) < self.topTrendScoreCount):
        return None
      return rankings
    return None

  def _savePrecomputedTrendScores(self, hashtag, ranking, cachedTrendScores):
    r_type = self.rank_types_and_versions[ranking]["type"]
    r_version = self.rank_types_and_versions[ranking]["version"]
    last_tweet_time = self.dbtime.getLastTime()
    rankings = {ranking:cachedTrendScores}
    row = self._loadPrecomputedTrendScores(hashtag, ranking, getWholeRow=True)
    if (row != None and
      row[3] != None and
      row[1] == r_version and
      row[2] == last_tweet_time):
      r_type = row[0] | r_type
      rankings = json.loads(row[3])
      rankings[ranking] = cachedTrendScores
    s_rankings = json.dumps(rankings)
    query = "UPDATE `hashtag_codes` SET `rank_type`=%(rt)s,`rank_version`=%(rv)s,`last_tweet_time`=%(ltt)s,`rankings`=%(r)s WHERE `id`='%(h)s'"
    data = {"rt":r_type, "rv":r_version, "ltt":last_tweet_time, "r":s_rankings, "h":hashtag}
    self.connection.sqlCall(query,data)

  # Get the tagTrendScore for all hour, days, and weeks
  # @param hashtag The hashtag id
  def tagTrendScoreAllTimes(self, hashtag, ranking="idf"):
    trendScores = {}

    # try to load precomputed trend scores
    cachedTrendScores = self._loadPrecomputedTrendScores(hashtag, ranking)
    if (cachedTrendScores != None):
      print("Loaded from database!")
      for rank in cachedTrendScores:
        dt = self.dbtime.toRealTime(rank[0])
        trendScores[dt] = rank[1]
      return trendScores
    cachedTrendScores = []

    # calculate trend scores fresh from database
    lastPercent = 1.1
    for hour in range(24):
      if (hour / 2.4 > lastPercent):
        print(str( int(lastPercent) * 10 ) + "%")
        lastPercent += 1
      for dayOfWeek in range(7):
        greatestWeek = self.dbtime.getGreatestWeek(hour,dayOfWeek)
        for week in range(greatestWeek):
          dbdt = self.dbtime.getDBTime(hour, dayOfWeek, week)
          dt = self.dbtime.toRealTime(dbdt)
          score = self.tagTrendScore(hashtag, hour, dayOfWeek, week)
          trendScores[dt] = score
          cachedTrendScores.append([dbdt, score])

    # cache top trend scores in the db
    orderedByScore = sorted(cachedTrendScores, key=lambda o: -o[1])[:self.topTrendScoreCount]
    cachedTrendScores2 = []
    for pair in orderedByScore:
      cachedTrendScores2.append(pair)
    self._savePrecomputedTrendScores(hashtag, ranking, cachedTrendScores2)

    return trendScores

  def getTopHashes(self, limit=100):
    self.connection.sqlCall("select * from hashtag_codes order by count desc limit "+limit+";")
    for row in self.connection.cursor:
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
    time = self.dbtime.getDBTime(hour, dayOfWeek, week)
    if (time < 0):
      return -1
    sph = 60 * 60
    return self.connection.countTweets(time, time+sph, [hashtag])

  # return A list of counts of hashtag occurances for all weeks of the given day/hour
  def allHashtagOccurances(self, hashtag, hour, dayOfWeek):
    greatestWeek = self.dbtime.getGreatestWeek(hour, dayOfWeek)
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
      results.append([x for x in self.connection.cursor][0][0])
      
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
    return ranges


def main():
  analysis = Analysis()
  ranges = analysis.get_valid_ranges()
  ranges = sorted(ranges, key=lambda r: r[2])
  print(ranges)


if __name__ == '__main__':
  main()