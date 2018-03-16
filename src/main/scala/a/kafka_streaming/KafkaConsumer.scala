package a.kafka_streaming

import java.io.IOException

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{InputDStream, DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


import redis.clients.jedis.{Jedis, JedisPool}


/**
  * Created by chaozhang on 2018/1/22.
  * 说明： 读取kafka的数据
  *
    自动维护kafka的offset，默认读取最新的offset，会丢失数据
  */
object KafkaConsumer {
  def main(args: Array[String]) {
    //当以jar包提交时，设置为local[*]会抛错（exitCode 13）。
    var masterUrl = "local[*]"
    if (args.length > 0){
      masterUrl = args(0)
    }

    //Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl)
      .setAppName("weike")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")


    //Kafka configurations
    val topics = Set("weike")
    val groupId = "test"
    val brokers = "ambari1:6667,ambari2:6667,ambari3:6667"

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serialize.class" -> "kafka.serialize.StringEncoder",
      "consumer.id" -> "1",
      "zookeeper.connect" -> "ambari2:2181,ambari3:2181,ambari1:2181",
      "group.id" -> groupId
    )



    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//    kafkaStream.print()
    //把数据缓存为两份
    kafkaStream.persist(StorageLevel.MEMORY_ONLY_2)


    kafkaStream.map(line =>line._2).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
