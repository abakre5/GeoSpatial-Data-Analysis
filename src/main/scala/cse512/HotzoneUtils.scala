package cse512

object HotzoneUtils {

  /**
   *
   * @param queryRectangle
   * @param pointString
   * @return boolean if points lies inside the rectangle or not
   */
  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    var queryRectangleArray = queryRectangle.split(",");
    var x1 = queryRectangleArray(0).trim().toDouble;
    var y1 = queryRectangleArray(1).trim().toDouble;
    var x2 = queryRectangleArray(2).trim().toDouble;
    var y2 = queryRectangleArray(3).trim().toDouble;

    var pointArray = pointString.split(",");

    var x = pointArray(0).trim().toDouble;
    var y = pointArray(1).trim().toDouble;

    var maxX = math.max(x1, x2);
    var maxY = math.max(y1, y2);
    var minX = math.min(x1, x2);
    var minY = math.min(y1, y2);

    return (x >= minX && y >= minY && x <= maxX && y <= maxY);
  }

  // YOU NEED TO CHANGE THIS PART IF YOU WANT TO ADD ADDITIONAL METHODS

}
