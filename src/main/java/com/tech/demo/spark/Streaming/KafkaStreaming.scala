package com.tech.demo.spark.Streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * @author xxx_xx
  * @date 2018/4/21
  */
object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kafka_wordcount").setMaster("yarn");
    val ssc = new StreamingContext(conf, Seconds(5));
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "centos-1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("data-syn")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => (record.key, record.value)).print()
    ssc.start();
    ssc.awaitTermination();
  }
}
