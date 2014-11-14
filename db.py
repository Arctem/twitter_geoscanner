import mysql.connector, ConfigParser

class Connection:
  cnx = None
  cursor = None

  def __init__(self):
    self.openConnection("mysql.conf")
    if (self.cnx != None):
      self.cursor = self.cnx.cursor()

  def sqlCall(self, query, data):
    try:
      retval = self.cursor.execute(query, data)
      return retval
    except mysql.connector.Error as err:
      print("Error: " + str(err))
      return 0

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
        database=conf.get("db", "database")
        )
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exists")
      else:
        print("Error: " + err)
    if (self.cnx.is_connected()):
      print "connected"
    else:
      print "not connected"
    return self.cnx

  # inserts the given tag into the database if it doesn't yet exist
  # increases the tag counter
  # @return the counter on the tag
  def increaseHashtagCount(self, hashtag):

    # check for empty string
    if len(hashtag) == 0:
      return 0

    # get the current count
    get_tag_count = "SELECT `count` FROM `hashtag_codes` WHERE `name`='%s'"
    num_rows = self.sqlCall(get_tag_count, (hashtag))
    if not num_rows:
      return -1
    num_rows = 0
    tag_count = 1
    for (count) in self.cursor:
      num_rows += 1
      tag_count = count + 1

    # insert/update the tag with the new count
    insert = ("INSERT INTO `hashtag_codes` (`name`,`count`) VALUES (%(name)s, %(count)s)")
    update = ("UPDATE `hashtag_codes` SET `count`=%(count)s WHERE `name`=%(name)s")
    data = { "name": hashtag, "count": tag_count }
    success = True
    if num_rows == 0:
      success = self.sqlCall(insert, data)
    else:
      success = self.sqlCall(update, data)
    if not success:
      return -1
    self.cnx.commit()
    return tag_count

  def tagToInt(self, hashtag):
    get_tag_id = "SELECT `id` FROM `hashtag_codes` WHERE `name`='%s'"
    if not self.sqlCall(get_tag_id, (hashtag)):
      return 0
    for (ident) in self.cursor:
      return ident
  
  def __tagsToInts(self, hashtagSet):
    for i in range(len(hashtagSet)):
      hashtagSet[i] = tagToInt(hashtagSet[i])

  # inserts the given tweet into the tweets table#
  # @param tweet should be formatted { geo: {lat: float, lon: float}, tags: [list of strings]}
  def insertTweet(self, tweet):
    tags = tweet.tags
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
    data = (time, tweet.lat, tweet.lon,
            tags[0], tags[1], tags[2], tags[3], tags[4],
            tags[5], tags[6], tags[7], tags[8], tags[9])
    if not self.sqlCall(insert, data):
      return False
    if not self.sqlCall(insert_compressed, data):
      return False
    return True

  def close(self):
    self.cursor.close()
    self.cnx.close()
    
def main():
  connection = Connection()
  for s in ("a", "a", "b", "c"):
    connection.increaseHashtagCount(s)
  
if __name__ == "__main__":
  main()
