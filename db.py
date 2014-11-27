import mysql.connector, ConfigParser
from datetime import datetime,timedelta

class dbTime:
  con = None

  def __init__(self, connection):
    self.con = connection

  # @return the index of the last week for the given hour and dayOfWeek
  def getGreatestWeek(self, hour, dayOfWeek):
    lastTweetTime = self.getLastTime()
    week = self.getWeek(lastTweetTime)
    if (dayOfWeek > self.getDayOfWeek(lastTweetTime) or 
      (hour > self.getHour(lastTweetTime) and dayOfWeek == self.getDayOfWeek(lastTweetTime))):
      week -= 1
    return week

  # @return the DB time of the first recorded tweet
  def getFirstTime(self):
    self.con.sqlCall("SELECT `time` FROM `tweets` ORDER BY `time` ASC LIMIT 1", {})
    for row in self.con.cursor:
      return int( row[0] )

  # @return the DB time of the last recorded tweet
  def getLastTime(self):
    self.con.sqlCall("SELECT `time` FROM `tweets` ORDER BY `time` DESC LIMIT 1", {})
    for row in self.con.cursor:
      return int( row[0] )

  # @return the week offset from Nov 1 to the given tweet time, where
  #   week0 = week containing Nov 
  #   week1 = following week
  #   ...
  def getWeek(self, tweetTime):
    spd = 60 * 60 * 24
    spw = spd * 7
    firstSunday = -6 * spd
    return (tweetTime - firstSunday) / spw

  # @return the dayOfWeek of the given tweet time
  def getDayOfWeek(self, tweetTime):
    return (self.toRealTime(tweetTime).weekday() + 1) % 7

  # @return the hour of the given tweet time
  def getHour(self, tweetTime):
    return self.toRealTime(tweetTime).hour

  # Get the database time for a given day, hour, and week offset from the first
  # recorded tweet time.
  # @param hour 0-23
  # @param dayOfWeek 0-6 (Sunday - Saturday)
  # @param week 0 - n (where n is number of weeks that have occured)
  # @return the DB time on success, -1 if any of the parameters are bad
  def getDBTime(self, hour, dayOfWeek, week):
    if (hour < 0 or hour > 23 or
      dayOfWeek < 0 or dayOfWeek > 6 or
      week < 0):
      return -1

    if (week > self.getGreatestWeek(hour, dayOfWeek)):
      return -1

    year = 2014
    month = 11
    firstTweetTime = self.getFirstTime()

    # calculate day of month
    day = self.getWeek(firstTweetTime) * 7 + dayOfWeek # if you assume Nov 1 is a Sunday
    day = day - 5 # except that Nov 1 is a Saturday
    day += week * 7

    # 30 days hath November
    if (day > 30):
      day -= 30
      month += 1

    return self.toDBTime(datetime(year,month,day,hour))

  # Converts a real time to database time
  # @param dt A datetime, as in datetime.now()
  # @return the datetime - Nov 1, 2014
  def toDBTime(self, dt):
    return int( (dt - datetime(2014,11,1)).total_seconds() )

  # converts a database time to a real datetime
  # @param dbTime The DB time to convert
  # @return The real time, as a datetime object
  def toRealTime(self, dbTime):
    spd = 60 * 60 * 24
    days = dbTime / spd
    seconds = dbTime % spd
    return datetime(2014,11,1) + timedelta(days, seconds)

class Connection:
  cnx = None
  cursor = None

  def __init__(self):
    self.openConnection("mysql.conf")
    if (self.cnx != None):
      self.cursor = self.cnx.cursor()

  # Execute a mysql call.
  # Get the resultant rows with a loop over Connection.cursor.
  # @param query String query in mysql form, with values encoded as %(valuename)s
  # @param data A dictionary of values to insert into the query string, eg {"valuename":"value"}
  # @return the returned value from cnx.cursor().execute(), or False on error
  def sqlCall(self, query, data):
    try:
      retval = self.cursor.execute(query, data)
      return retval
    except mysql.connector.Error as err:
      print("Error: " + str(err))
      return False

  # changes the connection to one specified by config_filename
  # @param config_filename see the example_mysql.conf file
  def openConnection(self, config_filename):
    errorcode = mysql.connector.errorcode
    conf = ConfigParser.RawConfigParser()
    conf.read(config_filename)
    try:
      self.cnx = mysql.connector.connect(
        user=conf.get("db", "user"),
        password=conf.get("db", "password"),
        host=conf.get("db", "host"),
        database=conf.get("db", "database"),
        autocommit=True
        )
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exists")
      else:
        print("Error: " + str(err))
    if (self.cnx.is_connected()):
      print "connected"
    else:
      print "not connected"
    return self.cnx

  # inserts the given tag into the database if it doesn't yet exist
  # increases the tag counter
  # @return the counter on the tag, or 0 on error
  def increaseHashtagCount(self, hashtag):

    # check for empty string
    if len(hashtag) == 0:
      return 0

    # get the current count
    get_tag_count = "SELECT `count` FROM `hashtag_codes` WHERE `name`=%(name)s"
    self.sqlCall(get_tag_count, {"name":hashtag})
    num_rows = 0
    tag_count = 1
    for row in self.cursor:
      count = row[0]
      num_rows += 1
      tag_count = count + 1

    # insert/update the tag with the new count
    insert = ("INSERT INTO `hashtag_codes` (`name`,`count`) VALUES (%(name)s, %(count)s)")
    update = ("UPDATE `hashtag_codes` SET `count`=%(count)s WHERE `name`=%(name)s")
    data = { "name": hashtag, "count": tag_count }
    if num_rows == 0:
      self.sqlCall(insert, data)
    else:
      self.sqlCall(update, data)
    return tag_count

  # Get the ID of a given hashtag string
  def tagToInt(self, hashtag):
    get_tag_id = "SELECT `id` FROM `hashtag_codes` WHERE `name`=%(name)s"
    self.sqlCall(get_tag_id, {"name":hashtag})
    rows = self.cursor.fetchall()
    if len(rows) == 0:
      return 0
    for row in rows:
      return row[0]

  def __tagsToInts(self, hashtagSet):
    for i in range(len(hashtagSet)):
      hashtagSet[i] = self.tagToInt(hashtagSet[i])

  # inserts the given tweet into the tweets table#
  # @param tweet should be formatted { geo: {lat: float, lon: float}, tags: [list of strings]}
  def insertTweet(self, tweet):
    tags = tweet["tags"]
    self.__tagsToInts(tags)
    tags += [0] * (10 - len(tags))
    time = int( (datetime.now() - datetime(2014,11,1)).total_seconds() )
    insert = ("INSERT INTO `tweets` "
              "(`time`,`lat`,`lon`,`tag0`,`tag1`,`tag2`,"
              " `tag3`,`tag4`,`tag5`,`tag6`,`tag7`,`tag8`,`tag9`) "
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    insert_compressed = ("INSERT INTO `tweets_compressed` "
              "(`time`,`lat`,`lon`,`tag0`,`tag1`,`tag2`,"
              " `tag3`,`tag4`,`tag5`,`tag6`,`tag7`,`tag8`,`tag9`) "
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    data = (time, tweet["geo"]["lat"], tweet["geo"]["lon"],
            tags[0], tags[1], tags[2], tags[3], tags[4],
            tags[5], tags[6], tags[7], tags[8], tags[9])
    self.sqlCall(insert, data)
    self.sqlCall(insert_compressed, data)

  def __performTweetQuery(self, isCount, startTime, endTime, hashtagIDs, limit, order):
    data = {"startTime":startTime, "endTime":endTime, "limit":limit, "order":order}
    hashtagIndex = 0
    for hashtagID in hashtagIDs:
      data["hid" + str(hashtagIndex)] = hashtagID
      hashtagIndex += 1

    s = "SELECT "
    if (isCount):
      s += "COUNT(*) AS 'count' "
    else:
      s += "* "
    s += " FROM `tweets`"
    if (startTime > -1 or endTime > -1 or hashtagID > -1):
      s += " WHERE"
      a = ""
      if (startTime > -1):
        s += " `time` >= %(startTime)s"
        a = " and"
      if (endTime > -1):
        s += a
        s += " `time` <= %(endTime)s"
        a = " and"
      hashtagIndex = 0
      for hashtagID in hashtagIDs:
        s += a
        s += " (`tag0`=%(hid" + str(hashtagIndex) + ")s"
        for i in range(1,10):
          s += " or `tag" + str(i) + "`=%(hid" + str(hashtagIndex) + ")s"
        s += ")"
        a = " and"
        hashtagIndex += 1
    s += " ORDER BY %(order)s"
    if (limit > -1):
      s += " LIMIT %(limit)s"

    self.sqlCall(s, data)

  # Get a list of tweets based on search criteria.
  # @param startTime The time of the tweet must be >= startTime,
  #     where epoch = Nov 1, 2014
  # @param endTime The time of the tweet must be <= startTime,
  #     where epoch = Nov 1, 2014
  # @param hashtagIDs The tweets must contain the given hashtag(s)
  # @param limit The maximum number of rows to select
  # @param order The value to order by
  # @return The set of rows from the tweets table that matches the given query.
  def selectTweets(self, startTime=-1, endTime=-1, hashtagIDs=[], limit=-1, order="`time` ASC"):
    self.__performTweetQuery(False, startTime, endTime, hashtagIDs, limit, order)
    return self.cursor.fetchall()

  # Get the number of tweets that match the given criteria
  # For parameters, see {@link selectTweets}
  def countTweets(self, startTime=-1, endTime=-1, hashtagIDs=[], limit=-1):
    self.__performTweetQuery(True, startTime, endTime, hashtagIDs, limit, "`time` AST")
    for row in self.cursor:
      return row[0]

  def close(self):
    self.cursor.close()
    self.cnx.close()

def main():
  connection = Connection()
  for s in ("a", "b", "c"):
    connection.increaseHashtagCount(s)
  connection.insertTweet({"geo": {"lat":1.1,"lon":2.2}, "tags": ["a", "b", "c"]})
  
if __name__ == "__main__":
  main()
