from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import numpy
from analyze_tweets import Analysis
from datetime import datetime, timedelta
import db
import sys
import argparse

# Example usage:
class Plotdata:
  def __init__(self):
    self.clear()

  def clear(self):
    self.serieses = []
    self.mapIsDrawn = False
    self.m = None #map

  # add a time-location series to be plotted
  # should be in the format
  # {
  #   "starttime": dbtime
  #   "endtime"  : dbtime
  #   "data"     : [
  #                 (time (int), latitude (double), longitude (double)),
  #                 ...
  #                ]
  #   "hashtag"  : {
  #                  "name": hashtag name,
  #                  "id": hashtag id,
  #                }
  # }
  def addSeries(self, series):
    if (len(series) == 0):
      print("Nothing in series")
      return False
    self.serieses.append(series)
    return True

  # draws the map
  def drawMap(self):
    if (len(self.serieses) == 0):
      print("There are no series to plot")
      return False

    # set up Kavrayskiy VII map projection
    # use low resolution coastlines
    m = Basemap(projection='kav7',lon_0=0,resolution='l')
    # draw coastlines, country boundaries, fill continents.
    m.drawcoastlines(linewidth=0.25,zorder=10)
    m.drawcountries(linewidth=0.25,zorder=10)
    m.fillcontinents(color='grey',lake_color='aqua',zorder=20)
    # draw the edge of the map projection region (the projection limb)
    m.drawmapboundary(fill_color='aqua',zorder=5)
    # draw lat/lon grid lines every 30 degrees.
    #m.drawmeridians(np.arange(0,360,30))
    #m.drawparallels(np.arange(-90,90,30))
    
    # draw each series
    for series in self.serieses:
      lons = [datapoint[2] for datapoint in series["data"]]
      lats = [datapoint[1] for datapoint in series["data"]]
      x, y = m(lons, lats)
      for i in range(len(x)):
        r = min( 1.0 / len(x) * i ,1.0)
        m.scatter([x[i]],[y[i]],30,marker='o',color=(r,0.0,0.0),zorder=30)

    # Get the name of the graph
    title = ""
    if (len(self.serieses) > 3):
      title += str(len(self.serieses)) + " series"
    else:
      title += "hashtags "
      seriesNames = [series["hashtag"]["name"] for series in self.serieses]
      title += ", ".join(seriesNames)
    title += " plotted for spread"
    plt.title(title)

    self.m = m
    self.mapIsDrawn = True
    return True

  # shows the map
  # also draws the map if not already drawn
  def showMap(self):
    if not self.mapIsDrawn:
      if not self.drawMap():
        print("Unable to draw map")
        return False
    self.plt.show()
    return True

  # saves the map to a file
  # also draws the map if not already drawn
  # @param filepath Should include the full filename, as well
  # eg: "/home/password/public_html/foo.png"
  def saveMap(self, filepath):
    if not self.mapIsDrawn:
      if not self.drawMap():
        print("Unable to save map")
        return False
    fig = plt.gcf()
    fig.set_size_inches(18.5,10.5)
    fig.savefig(filepath)

def main(args):
  analyzer = Analysis()
  dbtime = analyzer.dbtime
  connection = analyzer.connection

  hashtag = None
  hashtagName = None
  if (args.hashtagID is None):
    hashtag = connection.hashtagNameToID(args.hashtagName)
    hashtagName = args.hashtagName
  else:
    hashtagName = connection.hashtagToName(int(args.hashtagID))
    hashtag = int(args.hashtagID)
  if (hashtag is None):
    print "Hashtag not found"
    return 1
  print str(hashtag) + ": " + str(hashtagName)

  hour = 19
  dayOfWeek = 0
  greatestWeek = dbtime.getGreatestWeek(hour, dayOfWeek)
  top = 100

  orderedPairs = []
  series = analyzer.getSeriesToPlot(hashtag, ranking=args.ranking)
  plotdata = Plotdata()
  plotdata.addSeries(series)
  plotdata.saveMap("/home/password/public_html/"+ series["hashtag"]["name"] + ".png")
  print "plotted " + series["hashtag"]["name"]
  print ""

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Plot a graph of a hashtag\'s geographic spread.')
  parser.add_argument('--name', dest='hashtagName', type=str, default=None,
                      nargs='?', help='Specify the hashtag by name')
  parser.add_argument('--id', dest='hashtagID', default=None,
                      help='Specify the hashtag by id')
  parser.add_argument('--printTags', dest='printTags', default=False,
                      action='store_const', const=True,
                      help='Print the top 20 hashtags by populariy')
  parser.add_argument('--ranking', dest='ranking', default='idf',
                      help='Choose a ranking to sort by. One of idf or linear.')
  args = parser.parse_args()

  if args.printTags:
    analyzer = Analysis()
    topHashNames = [hashtagData[1] for hashtagData in analyzer.getTopHashes(20)]
    print topHashNames
  elif (args.hashtagID is not None or args.hashtagName is not None):
    main(args)
  else:
    args.print_help()