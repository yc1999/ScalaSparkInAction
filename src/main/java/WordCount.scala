import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    val inputFile =  "G:\\大三上\\云计算及应用\\presentation\\wordcount.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    textFile.foreach(println)
//    val wordCount = textFile.map(line => line.split(" "))
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCount.collect().foreach(println)
    wordCount.saveAsTextFile("aaa")
  }
}