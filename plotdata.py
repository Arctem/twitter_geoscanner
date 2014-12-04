from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import numpy
from analyze_tweets import Analysis
from datetime import datetime, timedelta
import db

# Example usage:
class Plotdata:
  def __init__(self):
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
    m.drawcoastlines(linewidth=0.25)
    m.drawcountries(linewidth=0.25)
    #m.fillcontinents(color='grey',lake_color='aqua')
    # draw the edge of the map projection region (the projection limb)
    m.drawmapboundary(fill_color='aqua')
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
        m.scatter([x[i]],[y[i]],30,marker='o',color=(r,0.0,0.0))

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

def main():
  analyzer = Analysis()
  plotdata = Plotdata()
  dbtime = analyzer.dbtime
  connection = analyzer.connection

  hashtag = 34 #jobs
  hour = 19
  dayOfWeek = 0
  greatestWeek = dbtime.getGreatestWeek(hour, dayOfWeek)
  top = 100

  for hashtagData in analyzer.getTopHashes():
    hashtag = hashtagData[0]
    orderedPairs = []
    series = analyzer.getSeriesToPlot(hashtag)
    plotdata.serieses = []
    plotdata.addSeries(series)
    plotdata.saveMap("/home/password/public_html/"+ series["hashtag"]["name"] + ".png")
    print "plotted " + series["hashtag"]["name"]
    print ""

if __name__ == '__main__':
  main()