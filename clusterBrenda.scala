import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

case class point(longitude: Double, latitude: Double) {
  def euclideanDistance(x1:Double, y1:Double, x2:Double, y2:Double): Double = {
    val distance = scala.math.pow((x2 - x1), 2) + scala.math.pow((y2 - y1), 2)
    scala.math.sqrt(distance)
  }

  def distance(point2:point) = euclideanDistance(this.longitude, this.latitude, point2.longitude, point2.latitude)
}

case class clusterDetail(longitude: Double, latitude: Double, predict: Int)
case class clusterCentroids(predictC:Int, longitudeC: Double, latitudeC: Double, sizeC:Long)

case class dataTaxiPoint(longitude: Double, latitude: Double)

object clusterBrenda {
  def main(args:Array[String]): Unit = {
    //configuration
    val conf = new SparkConf()
    conf.setMaster("local[4]") // you need to comment this for isabelle
    conf.setAppName("bordonez")
    //val sc = new SparkContext(conf)
    val session = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._

    val taxiData = session.read.option("header", true).option("delimiter", ",")
      .csv("taxiTrips-small.csv")
      //.csv("/data/TaxiTrips/taxiTrips-medium.csv") //** medium sample this is for isabelle
      //.csv("/data/TaxiTrips/taxiTrips-big/taxiTrips-big.csv") //** big sample this is for isabelle
      .filter(element=>element.getString(10) != null || element.getString(10) != "" || element.getString(11) != "" || element.getString(11) != "")
      .map(element => dataTaxiPoint(element.getString(10).toDouble,element.getString(11).toDouble)).persist()
    println(" -------------------- naive kmeans --------------------")
    // naive kmeans
    val x = kmeans(taxiData, 10, session)
    x._2.show()
    // bisecting kmeans
    println(" -------------------- bisecting kmeans --------------------")
    val y = bisectingKmeans(taxiData, 10, session)
    y._2.show()

    session.stop()

  }

  def kmeans(data: Dataset[dataTaxiPoint], k:Int, session: SparkSession): (Dataset[clusterDetail],Dataset[clusterCentroids]) = {
    import session.implicits._
    // STEP 1: Initiate with a random sample of k points
    var centroids = initSampleCluster(data, k)
    // STEP 2: Assign each dataPoint to the nearest cluster
    var clustering = assign(data, centroids)
    clustering.createOrReplaceTempView("cluster")
    // STEP 3: Group clusters according to their size for comparison
    var s = clustering.sqlContext.sql("SELECT predict, COUNT(*) AS size FROM cluster GROUP BY predict ORDER BY predict").map(row => (row.getInt(0),row.getLong(1))).collect().toList
    var difference:List[(Int,Long)] = List()
    do {
      // STEP 4: Update with new centroids of each cluster
      centroids = update(clustering, k, session)
      // STEP 5: Assign each dataPoint to the new nearest cluster
      clustering = assign(data, centroids)
      // STEP 6: Group clusters according to their size for comparison
      var s2 = clustering.sqlContext.sql("SELECT predict, COUNT(*) AS size FROM cluster GROUP BY predict ORDER BY predict").map(row => (row.getInt(0),row.getLong(1))).collect().toSet
      // STEP 7: Compare the two lists to see if there's a difference
      difference = s.filterNot(s2)
      s = clustering.sqlContext.sql("SELECT predict, COUNT(*) AS size FROM cluster GROUP BY predict ORDER BY predict").map(row => (row.getInt(0),row.getLong(1))).collect().toList
      //STEP 8: Go to step 4 if there’s a difference
    } while (difference.length > 0)
    // STEP 9: Build the summary table to see the results
    val dataPointCentroid = centroids.zipWithIndex.map(element => clusterCentroids(element._2, element._1.longitude, element._1.latitude, 0)).toList
    val temp = s zip dataPointCentroid
    val result = temp.map(element => clusterCentroids(element._2.predictC, element._2.longitudeC,element._2.latitudeC, element._1._2)).toSeq.toDS()
    (clustering,result)
  }

  def initSampleCluster(x: Dataset[dataTaxiPoint], k:Int) : Array[point] = {
    //x.sample(fraction = 0.01).take(k).map(element => point(element.longitude,element.latitude))
    x.rdd.takeSample(false,k).map(element => point(element.longitude,element.latitude))
  }
  //assign to the nearest point
  def assign(data:Dataset[dataTaxiPoint], m:Array[point]): Dataset[clusterDetail] = {
    val clustering: Dataset[clusterDetail] = data.map(pickup=> {
      val centroid = m.minBy(_.distance(point(pickup.longitude,pickup.latitude)))
      val index = m.indexOf(centroid)
      clusterDetail(pickup.longitude,
        pickup.latitude,
        index)
    }
    )(Encoders.product[clusterDetail])
    clustering
  }
// recenter and assign
  def update(x:Dataset[clusterDetail], k:Int, session: SparkSession) : Array[point] = {
    import session.implicits._
    //set table name
    x.createOrReplaceTempView("cluster")
    val table = x.sqlContext.sql("SELECT sum(longitude) AS longitude, sum(latitude) AS latitude, count(*) AS length FROM cluster GROUP BY predict")
    //getting the average
    val average = table.withColumn("avgLongitude",col("longitude") / col("length"))
    val average2 = average.withColumn("avgLatitude",col("latitude") / col("length"))
    average2.map(row => point(row.getDouble(3),row.getDouble(4))).collect()
  }

  def bisectingKmeans(data: Dataset[dataTaxiPoint], k:Int, session: SparkSession): (Dataset[clusterDetail],Dataset[clusterCentroids]) = {
    import session.implicits._
    // STEP 1: Group with k-means, k = 2
    var clusterI = kmeans(data, 2, session)
    var k2: Int = 2
    var clusterR:Dataset[clusterDetail] = session.emptyDataset
    var clusterCentroidR:Dataset[clusterCentroids] = session.emptyDataset
    // STEP 2: Assign the result to our main return Variable
    var clusterTemp = assignNewCluster(clusterR, clusterCentroidR,clusterI._1, clusterI._2)
    clusterR = clusterTemp._1
    clusterCentroidR = clusterTemp._2
    // STEP 3: Go to step 4 while k2 it’s the same as k
    while(k2 < k){
      // STEP 4: Find the biggest cluster
      val max = distanceCluster(clusterR, clusterCentroidR)
      val chosenCluster = clusterR.filter(col("predict") === max).map(element => dataTaxiPoint(element.longitude, element.latitude))
      // STEP 5: Delete the biggest Cluster from main return Variable
      // **  NOTE: THIS IS A CHANGE FROM THE ISABELLE'S VERSION. MAP WAS COMMENTED BECAUSE IT WAS REDUNDANT.
      clusterR = clusterR.filter(element => element.predict != max)//.map(element => clusterDetail(element.longitude, element.latitude, element.predict))
      clusterCentroidR = clusterCentroidR.filter(element => element.predictC != max)//.map(element => clusterCentroids(element.predictC, element.longitudeC, element.latitudeC, element.sizeC))
      // STEP 6: Group with k-means the biggest cluster, k = 2
      clusterI = kmeans(chosenCluster, 2, session)
      // STEP 7: Assign result to our main return variable
      clusterTemp = assignNewCluster(clusterR, clusterCentroidR,clusterI._1, clusterI._2)
      clusterR = clusterTemp._1
      clusterCentroidR = clusterTemp._2
      // increment
      k2 = k2 + 1
    }
    // STEP 8: Return result
    (clusterR,clusterCentroidR)
  }

  def assignNewCluster(data: Dataset[clusterDetail], centroid: Dataset[clusterCentroids],data2: Dataset[clusterDetail], centroid2: Dataset[clusterCentroids]): (Dataset[clusterDetail],Dataset[clusterCentroids]) = {
    centroid.createOrReplaceTempView("bisecting")
    // getting last index from data/centroid
    var index = -1
    if (!centroid.isEmpty)
      index = centroid.sqlContext.sql("SELECT predictC FROM bisecting ORDER BY predictC DESC").first().getInt(0)
    // Reassign new index for data2 and centroid2.
    val data2NewIndex = data2.map(element => {
      if (element.predict == 0)
        clusterDetail(element.longitude, element.latitude, index + 1)
      else
        clusterDetail(element.longitude, element.latitude, index + 2)
    })(Encoders.product[clusterDetail])
    val centroid2NewIndex = centroid2.map(element => {
      if (element.predictC == 0)
        clusterCentroids(index + 1, element.longitudeC, element.latitudeC, element.sizeC)
      else
        clusterCentroids(index + 2, element.longitudeC, element.latitudeC, element.sizeC)
    })(Encoders.product[clusterCentroids])
    // adding new groups
    val dataNew = data.union(data2NewIndex)
    val centroidNew = centroid.union(centroid2NewIndex)
    (dataNew,centroidNew)
  }

  def distanceCluster(data: Dataset[clusterDetail], centroid: Dataset[clusterCentroids]): Int = {
    // return the ID of the biggest cluster (the greatest distance)
    val distanceMax = data.join(centroid,data("predict") === centroid("predictC"), "inner")
    distanceMax.createOrReplaceTempView("bisecting")
    //distanceMax.sqlContext.sql("SELECT predict, SUM(( POWER((longitude - longitudeC),2)  + POWER((latitude - latitudeC),2) ))  AS distance FROM bisecting GROUP BY predict ORDER BY distance DESC").first().getInt(0)
    distanceMax.sqlContext.sql("SELECT predict, COUNT(*)  AS total FROM bisecting GROUP BY predict ORDER BY total DESC").first().getInt(0)
  }

}
