package a.utils

import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis


/**
  * Created by chaozhang on 2018/1/18.
  * spark 读写redis的数据
  *
  * 1:读取redis的数据
  * 2：写入redis的数据
  *
  * 备注：使用的是原始的Jedis
  */
object ProduceDataToRedis {
  private val redisHost = "192.168.1.11"
  private val redisPort = 6379
  private val redisPassword = "admin123"


  def main(args: Array[String]) {


    val conf = new SparkConf()
      .setAppName("RedisToSpark")
        .setMaster("local[*]")

    val sc = new SparkContext(conf)
    var jd = new Jedis(redisHost,redisPort )
    jd.auth(redisPassword)
    jd.select(15)

//    readRedis(sc, jd)
     write(sc, jd)


  }

  //写数据到redis里面
  def write(sc: SparkContext, jd: Jedis): Unit ={
    var i = 0
    while(true){

      jd.lpush("2018", "斯特" + i )
      println("write to redis value is " + "斯特" + i)
      Thread.sleep(4000)
      i = i+1
    }
  }


  // 读取redis的数据
  def readRedis(sc: SparkContext, jd: Jedis): Unit = {
    jd.auth(redisPassword)

    val str = jd.get("a_1")
    var strList = str.split(",")
    val a = sc.parallelize(strList, 3)
    val b = a.keyBy(_.length)
    b.collect().foreach(s => println(s._1 + ":" + s._2))


    sc.stop()
  }


}
