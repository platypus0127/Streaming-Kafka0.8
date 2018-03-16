package a.spark_streaming

import com.redislabs.provider.redis.streaming.RedisInputDStream
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.redislabs.provider.redis._
import redis.clients.jedis.Jedis
import a.utils.{ConnectPoolUtil}



/**
  * Created by chaozhang on 2018/1/18.
  * SSC 实时读取redis里的数据。
  * 使用的是spark-redis。
  */
object RedisDemo {
  private val redisHost = "192.168.1.11"
  private val redisPort = 6379
  private val redisPassword = "admin123"
  private val redisDB = 15

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("RedisDemo")
      .set("redis.host", redisHost)
      .set("redis.port", redisPort.toString)
      .set("redis.auth", redisPassword)
      .set("redis.db", redisDB.toString)// 默认读取的是0库

    val ssc = new StreamingContext(conf ,Seconds(2))
    ssc.sparkContext.setLogLevel("WARN")

    // 模糊匹配key值,找出对应key的数量
    val listRDD = ssc.sparkContext.fromRedisList("2018*")
    println("count:" + listRDD.count().toInt  )

    // 构建DStream
    val redisStream = ssc.createRedisStream(Array("2018"), storageLevel = StorageLevel.MEMORY_AND_DISK_2)
  //  val redisStream_2 = ssc.createRedisStreamWithoutListname(Array("2018", "bar"), storageLevel = StorageLevel.MEMORY_AND_DISK_2)
    val valuesStream = redisStream.map(_._2)

    // 把键与值存入数据
    storeMysql(redisStream)

    // 存入redis
   // storeRedis(valuesStream)


    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 存入数据到redis
    *
    * @param valuesStream
    */
  def storeRedis(valuesStream: DStream[String]): Unit = {
    print("开始存入到redis")
    valuesStream.print()
    valuesStream.foreachRDD(rdd =>
      rdd.foreachPartition(eachPart =>{
        val jedis = new Jedis(redisHost, redisPort)
        jedis.auth(redisPassword)
        jedis.select(redisDB)

        eachPart.foreach(name => {
          val name_2 = "巴里" + name
          jedis.lpush("2019", name_2)
          print(name_2)

        })
        jedis.close()

      })

    )
  }


  /**
    * 把结果数据存入到mysql
    *
    * @param redisStream
    */
  def storeMysql(redisStream: RedisInputDStream[(String, String)]): Unit = {
    //注意：foreachRDD方法的调用，该方法运行于driver之上，如果将数据库连接放在该方法位置会导致连接运行在driver上，
    //会抛出connection object not serializable的错误。因此需要将数据库连接方法创建在foreach方法之后，需要注意的是这种做法还需要优化，
    // 因为这样会对每个rdd记录创建数据库连接，导致系统运行变慢，可以通过先调用foreachPartition方法为每个分区单独重建一个数据库连接然后再该分区之内再遍历rdd记录。
    // 这样可以减少数据库连接的创建次数，还可以通过构建数据库连接池的方法继续优化。
    redisStream.foreachRDD(rdd => {
      rdd.foreachPartition(eachPartition => {

        val conn = ConnectPoolUtil.getConnection
        if (conn != null) {
          // 这个地方居然报异常？？？
          conn.setAutoCommit(false) // 手动提交
          val stmt = conn.createStatement()
          eachPartition.foreach(name => {
            println("name:" + name)
            stmt.addBatch("insert into streaming values('" + name + "')")
          })
          stmt.executeBatch()
          conn.commit()
          conn.close()
        }
      }
      )
    })
  }
}
