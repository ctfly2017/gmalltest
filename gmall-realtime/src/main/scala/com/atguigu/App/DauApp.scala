package com.atguigu.App

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.Bean.StartUpLog
import com.atguigu.GmallConstants
import com.atguigu.Handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {
    //todo 1. 创建SparkConf
    //todo 2. 创建Streamingcontext
    //todo 3. 链接kafka
    //todo 4. 将json数据转换为样例类，方便解析
    //todo 5. 进行批次间去重
    //todo 6. 进行批次内去重
    //todo 7. 将去重后的数据保存至redis中，方便下次去重使用
    //todo 8. 将数据保存至hbase

    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    val startUpLogDstream: DStream[StartUpLog] = kafkaDstream.mapPartitions(partition => {
      partition.map(record => {
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

        val str: String = sdf.format(new Date(startUpLog.ts))

        startUpLog.logDate = str.split(" ")(0)

        startUpLog.logHour = str.split(" ")(1)

        startUpLog

      })

    })


    val filterByRedisDstream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDstream, ssc.sparkContext)

    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterbyGroup(filterByRedisDstream)

    DauHandler.saveMidToRedis(startUpLogDstream)

    filterByGroupDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL1109_DAU", Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE",
        "LOGHOUR", "TS"), HBaseConfiguration.create(), Some("hadoop102,hadoop103,hadoop104:2181"))

    })
    ssc.start()
    ssc.awaitTermination()
  }
}
