import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object PageRank {
  def main(args: Array[String]) {
    val inputFile1 =  "G:\\大三上\\云计算及应用\\presentation\\txt1.txt"
    val inputFile2 = "G:\\大三上\\云计算及应用\\presentation\\txt2.txt"

    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val textFile1 = sc.textFile(inputFile1) //BaseRDD
    val textFile2 = sc.textFile(inputFile2) //BaseRDD

    val links = sc.textFile(inputFile1).map(line => {
      val tmp = line.split(" ")
      (tmp(0),tmp(1).split(",").toList) //元组下标是从1开始的
    })

    links.foreach(println)

    var ranks = sc.textFile(inputFile2).map(line => {
      val tmp = line.split(" ")
      (tmp(0), tmp(1).toDouble) //元组下标是从1开始的
    })
//
//    val ITERATIONS = 100
//
//    for (i <- 1 to ITERATIONS) {
//      val result = links.join(ranks).flatMap({
//        case (url, (links, rank)) =>
//          links.map(dest => (dest,rank / links.size))
//      })
//      ranks = result.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)

//    import org.apache.spark.HashPartitioner
//
//    val links = sc.parallelize(List(("A",List("B","C")),("B",List("A","C")),("C",List("A","B","D")),("D",List("C")))).partitionBy(new HashPartitioner(100)).persist()
//
//    var ranks=links.mapValues(v=>1.0)
//
    for (i <- 0 until 100) {
      val contributions=links.join(ranks).flatMap {
        case (pageId,(links,rank)) => links.map(dest=>(dest,rank/links.size))
      }
      ranks = contributions.reduceByKey((x,y)=>x+y).mapValues(v=>0.15+0.85*v)
    }

    ranks.sortByKey().collect()
    ranks.foreach(println)
  }
}
