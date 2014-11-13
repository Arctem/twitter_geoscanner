import mysql.connector, ConfigParser

class Connection:
  cnx = None
  
  def __init__(self):
    self.cnx = self.openConnection("mysql.conf")
    
  def openConnection(self, config_filename):
    conf = ConfigParser.RawConfigParser()
    conf.read(config_filename)
    self.cnx = mysql.connector.connect(
      user=conf.get("db", "user"),
      password=conf.get("db", "password"),
      host=conf.get("db", "host"),
      database=conf.get("db", "database")
      )
    
def main():
  connection = Connection()
  
if __name__ == "__main__":
  main()
