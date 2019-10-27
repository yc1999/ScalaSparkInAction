import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer

object PeopleYouMightKnow {

  def main(args: Array[String]) {
    val inputFile =  "G:\\大三上\\云计算及应用\\presentation\\LiveJournal.txt"
    System.setProperty("hadoop.home.dir","G:\\大三上\\云计算及应用\\presentation\\hadoop-common-2.2.0-bin-master" )

    val conf = new SparkConf().setAppName("PeopleYouMightKnow").setMaster("local")
    val sc = new SparkContext(conf)
    val link = sc.textFile(inputFile).flatMap(line => { //这些不会执行，直到action被执行！！！
      val arr = ArrayBuffer[(Int,String)]()  //声明一个可变长数组，用于放置元组
      val input = line.split("\t")
      if (input.size == 2){
        val targets = input(1).split(",")
        for (i <- 0 to targets.size - 1){
          val tmp = (input(0).toInt,"0_"+targets(i))
          arr.append(tmp)
        }
        for (i <- 0 to targets.size - 1){
          for (j <-  i+1 to targets.size - 1){
            val tmp1 = (targets(i).toInt,"1_"+targets(j))
            arr.append(tmp1)
            val tmp2 = (targets(j).toInt,"1_"+targets(i))
            arr.append(tmp2)
          }
        }
      }
      arr //flatMap可以返回序列，这是我们需要的
    })
//    link.foreach(println)


    val friend = link.groupByKey()
//    friend.foreach(line => println(line._1+"的序列"+line._2))

    val sortedFriend = friend.sortBy(_._1)

    val test = sortedFriend.map({ //没有传参了,后面是case
      case (source,friends) =>
//        val mutualFriends:Map[Int,Int] = Map() //创建可变长的Map
        val mutualFriends = new scala.collection.mutable.HashMap[Int,Int]
//        println("正在处理"+source+"的朋友们：")

        val friendsArr = friends.toArray
        for (i <- 0 to friendsArr.size-1){
          val check = friendsArr(i).startsWith("0_")
          val target = friendsArr(i).substring(2).toInt;

          if (check){
            //如果当前读入表示是直接朋友，那么设置值为-1
              mutualFriends.put(target,-1)
          }
          else{
            //如果当前读入表示不是直接朋友，判断是否在Map中
            if (mutualFriends.contains(target)){
              //判断之前读入是否是直接朋友，不是则加一
              if (mutualFriends.get(target).getOrElse(-1) != -1){ //神坑，我太菜了，呜呜呜
//                print(target + " ")
                val a = mutualFriends.get(target).getOrElse(0) + 1
                mutualFriends.put(target,a)
//                println(mutualFriends.get(target))
              }
            }else{
              mutualFriends.put(target,1)
            }
          }
        }
        mutualFriends.foreach(println)

        //按key从大到小排序，这一有点问题o
        val filterMapSortSmall = mutualFriends.toList.sortBy(_._1)

        //按value从大到小排序
        val mapSortBig = filterMapSortSmall.sortBy(-_._2)
//        mapSortBig.foreach(line => println(line._1+"\t"+line._2))



        //按value！=-1筛选
        val filterMapSortBig = mapSortBig.filter(line => line._2 != -1)

//        filterMapSortBigSmall.foreach(line => println(line._1+"\t"+line._2))

        val finalFriends = filterMapSortBig.map(line => line._1)
//        println("最终列表")
//        finalFriends.foreach(println)

        //取前十个用户,这个操作有点骚
        val take = finalFriends take 10

        (source,take)
    })
//    test.foreach(println) //必须要有action才会执行
    //还要转换为string
       val finalOutput =    test.map(line => {
              var output = line._1.toString() + '\t'
              for (i <- 0 to line._2.size - 1){
                if(i == 0){
                  output += line._2(i).toString()
                }
                else{
                  output += ","+line._2(i).toString()
                }
              }
              output
            })
    finalOutput.saveAsTextFile("G:\\大三上\\云计算及应用\\presentation\\output")
  }
}