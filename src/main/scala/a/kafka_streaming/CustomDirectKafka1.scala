package a.kafka_streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.lang.time.StopWatch
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange, KafkaUtils}
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mayn on 2018/3/5.
  *Spark Streaming消费Kafka Direct方式数据零丢失实现
  *
  * 使用场景：
  * Spark Streaming实时消费kafka数据的时候，程序停止或者Kafka节点挂掉会导致数据丢失，Spark Streaming也没有设置CheckPoint（据说比较鸡肋，虽然可以保存Direct方式的offset，但是可能会导致频繁写HDFS占用IO），
  * 所以每次出现问题的时候，重启程序，而程序的消费方式是Direct，所以在程序down掉的这段时间Kafka上的数据是消费不到的，虽然可以设置offset为smallest，但是会导致重复消费，
  * 重新overwrite hive上的数据，但是不允许重复消费的场景就不能这样做。
  *
  * 原理阐述：
  * 1.基于 Receiver-based 的 createStream 方法。receiver从Kafka中获取的数据都是存储在Spark Executor的内存中的，然后Spark Streaming启动的job会去处理那些数据。然而，在默认的配置下，这种方式可能会因为底层的失败而丢失数据。
  * 如果要启用高可靠机制，让数据零丢失，就必须启用Spark Streaming的预写日志机制（Write Ahead Log，WAL）。该机制会同步地将接收到的Kafka数据写入分布式文件系统（比如HDFS）上的预写日志中。
  * 所以，即使底层节点出现了失败，也可以使用预写日志中的数据进行恢复。本文对此方式不研究，有兴趣的可以自己实现，个人不喜欢这个方式。KafkaUtils.createStream

  * 2.Direct Approach (No Receivers) 方式的 createDirectStream 方法，但是第二种使用方式中  kafka 的 offset 是保存在 checkpoint 中的，如果程序重启的话，会丢失一部分数据，我使用的是这种方式。
  * KafkaUtils.createDirectStream。本文将用代码说明如何将 kafka 中的 offset 保存到 zookeeper 中，以及如何从 zookeeper 中读取已存在的 offset。
  *
  * 错误记录：原因是
  * 18/03/13 16:15:48 ERROR JobScheduler: Error running job streaming job 1520928946000 ms.0
    java.lang.IllegalArgumentException: Path must start with / character at org.apache.zookeeper.common.PathUtils.validatePath(PathUtils.java:51)
  *
  */
object CustomDirectKafka1 {


  def main(args: Array[String]) {
    val masterURL = "local[2]"
    if (args.length > 0) {
      System.exit(0)
    }

    //Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterURL)
      .setAppName("weike_3")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.sparkContext.setLogLevel("ERROR")

    //Kafka configurations
    val topic = "weike_3"
    val brokers = "spark-0:6667,spark-1:6667,spark-2:6667"
    val groupId = "bigdata"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
 /*     "serialize.class" -> "kafka.serialize.StringEncoder"*/
      "zookeeper.connect" -> "spark-0:2181,spark-1:2181"
    )
    var offsetRanges = Array[OffsetRange]()

    //Zookeeper 的配置信息
    val zkHost = "spark-0"
    val zkClient: ZkClient = new ZkClient(zkHost)
    val topicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(groupId, topic) // 创建一个ZKGroupTopicDirs 对象

    println("topicDirs:" + topicDirs.consumerOffsetDir)
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}") // 查询该路径下是否有子节点（默认有字节点为我们自己保存不同 partition 时生成的）

    var fromOffsets:Map[TopicAndPartition, Long] = Map() //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置

    var kafkaStream: InputDStream[(String, String)] = null

    if(children > 0){//如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
      for(i <- 0 until children){
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
//        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")

        val tp = TopicAndPartition(topic, i)
        fromOffsets += (tp -> partitionOffset.toLong) //把不同partition 对应的offset 增加到 fromOffSets中
      }
      val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message()) // 这个会将kafka 的消息进行transform, 最终kafka的数据都会变成（topic_name, messsage）这样的tuple
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
      kafkaStream.print()
    }
    else {// 没有保存过offset,根据kafkaParam 的配置使用最新的或者最旧的offset
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams,Set(topic))
      kafkaStream.print()
    }

    kafkaStream.transform{ rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges ///得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }.map(_._2).foreach(rdd =>{
      for (o <- offsetRanges){
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        println(zkPath + o.fromOffset.toString)
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString) //将该 partition 的 offset 保存到 zookeeper
      }
//      rdd.foreach(s => println(s))
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
