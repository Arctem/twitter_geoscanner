from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot
import numpy

# Example usage:
class plotdata:
  serieses
  mapIsDrawn
  plt
  
  def __init__(self):
    self.serieses = []
    self.mapIsDrawn = False
    self.plt = None
    self.map = None

  # add a time-location series to be plotted
  # should be in the format
  # [
  #   [time (int), latitude (double), longitude (double)],
  #   ...
  # ]
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
    # TODO
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