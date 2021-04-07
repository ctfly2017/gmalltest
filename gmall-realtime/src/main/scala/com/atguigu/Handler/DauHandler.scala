package com.atguigu.Handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.Bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  def filterbyGroup(filterByRedisDstream: DStream[StartUpLog]) = {
    val value: DStream[StartUpLog] = {
      val midAndDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDstream.mapPartitions(partition => {

        partition.map(log => {
          ((log.mid, log.logDate), log)
        })
      })
      val midAndDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndDateToLogDStream.groupByKey()

      val midAndDateToLogListDStream: DStream[((String, String), List[StartUpLog])] = midAndDateToLogIterDStream.mapValues(iter => {
        iter.toList.sortWith(_.ts < _.ts).take(1)
      })
      midAndDateToLogListDStream.flatMap(_._2)
    }
    value

  }

  def filterByRedis(startUpLogDstream: DStream[StartUpLog],sc: SparkContext) ={

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val value: DStream[StartUpLog] = startUpLogDstream.transform(rdd => {
      val jedisClient: Jedis = RedisUtil.getJedisClient

      val rediskey: String = "DAU" + sdf.format(new Date(System.currentTimeMillis()))

      val mids: util.Set[String] = jedisClient.smembers(rediskey)

      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      val midFilterRdd: RDD[StartUpLog] = rdd.filter(log => {
        !midBC.value.contains(log.mid)
      })

      jedisClient.close()
      midFilterRdd

    })
    value


  }





  def saveMidToRedis(startUpLogDstream: DStream[StartUpLog]): Unit = {
    startUpLogDstream.foreachRDD(rdd =>{
    rdd.foreachPartition(partition =>{

    val jedisClient: Jedis = RedisUtil.getJedisClient

      partition.foreach(log=>{
      val rediskey: String = "DAU"+log.logDate

        jedisClient.sadd(rediskey,log.mid)
      })

      jedisClient.close()

    })
    })

  }

}
