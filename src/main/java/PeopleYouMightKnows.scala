package org.csu
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.control.Breaks

object PeopleYouMightKnows {

  class SortPair(val Id : String, val count : Long) extends Ordered[SortPair] with Serializable{
    override def compare(that: SortPair): Int = {
      val res = that.count.compareTo(count)
      if(res != 0)
        res
      else
        Id.toLong.compareTo(that.Id.toLong)
    }
  }

  def lineMap(line : String) : List[(String, String)] = {
    var result = List[(String, String)]()       //存储间接朋友对
    val content = line.split("\t");     //0     1,2,3,4,5,6,7,8,9
    val source = content(0)                     //0

    if(content.length < 2){                     //表明该行是0           这种形式
      result ::= (source, "empty")              //防止source是个孤立点
      return result
    }

    //如果该行里有直接朋友关系就按","将其分隔开
    val friendList = content(1).split(",")

    for(i <- 0 to friendList.length - 1){
      result ::= (source, "T" + friendList(i))   //标记已经是真正的朋友了
      result ::= (friendList(i), "T" + source)
      for(j <- i + 1 to friendList.length - 1){
        result ::= (friendList(i), friendList(j)) //记录可能的间接朋友对
        result ::= (friendList(j), friendList(i))
      }
    }
    result
  }

  def lineReduce(source: String, x:Iterable[String]) : String = {
    var m = mutable.HashMap[String, Long]()
    for(s <- x){
      if(s.startsWith("T")){
        m.put(s.substring(1), -1)
      }else if(m.contains(s) && m.get(s).get != -1){
        m.put(s, m.get(s).get + 1)
      }else{
        if(!s.equals("empty")){
          m.put(s, 1)
        }
      }
    }

    val sortedSet = m.toList.map(e => new SortPair(e._1, e._2)).sorted.take(10)

    val result = new StringBuilder("  ")
    for(i <- 0 to sortedSet.size - 1){
      if(sortedSet(i).count == -1) new Breaks().break()
      if(i < sortedSet.size - 1 && sortedSet(i + 1).count != -1){
        result.append(sortedSet(i).Id).append(",")
      }else{
        result.append(sortedSet(i).Id)
      }
    }

    result.toString()
  }

  def main(args: Array[String]): Unit = {
    if(args.length < 2){
      System.err.println("The Usage of arguments must be args(0) and args(1)")
      System.exit(-1)
    }
    val conf = new SparkConf().setAppName("PeopleYouMightKnows")
    val sc = new SparkContext(conf)
    sc.textFile(args(0)).flatMap(line => lineMap(line)).groupByKey().sortBy(_._1.toInt).map(x => (x._1, lineReduce(x._1, x._2))).saveAsTextFile(args(1))
  }

}

