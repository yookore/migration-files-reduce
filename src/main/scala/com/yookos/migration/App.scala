package com.yookos.migration

import akka.actor.{ Actor, Props, ActorSystem, ActorRef }
import akka.pattern.{ ask, pipe }
import akka.event.Logging
import akka.util.Timeout

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming.{ Milliseconds, Seconds, StreamingContext, Time }
import org.apache.spark.streaming.receiver._

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.mapper._
import com.datastax.spark.connector.cql.CassandraConnector

import org.json4s._
import org.json4s.JsonDSL._
//import org.json4s.native.JsonMethods._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import org.apache.commons.lang.StringEscapeUtils
import org.joda.time.DateTime

/**
 * @author ${user.name}
 */
object App extends App {
  
  // Configuration for a Spark application.
  // Used to set various Spark parameters as key-value pairs.
  val conf = new SparkConf(false) // skip loading external settings
  
  val mode = Config.mode
  Config.setSparkConf(mode, conf)
  val cache = Config.redisClient(mode)
  val sc = new SparkContext(conf)
  
  if (mode != "local") {
    sc.addJar("hdfs:///user/hadoop-user/data/jars/postgresql-9.4-1200-jdbc41.jar")
    sc.addJar("hdfs:///user/hadoop-user/data/jars/migration-binary-reduce-0.1-SNAPSHOT.jar")
  }

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val system = SparkEnv.get.actorSystem
  
  createSchema(conf)

  implicit val formats = DefaultFormats

  createSchema(conf)
  
  // serializes objects from redis into
  // desired types
  //import com.redis.serialization._
  //import Parse.Implicits._
  
  val keyspace = Config.cassandraConfig(mode, Some("keyspace"))
  var count = new java.util.concurrent.atomic.AtomicInteger(0)
  val totalLegacyUsers = 2124155L
  var cachedIndex = if (cache.get("latest_legacy_files_index") == null) 0 else cache.get("latest_legacy_files_index").toInt

  // Using the mappings table, get the profiles of
  // users from 192.168.10.225 and dump to mongo
  // at 10.10.10.216
  val mappingsDF = sqlContext.load("jdbc", Map(
      "url" -> Config.dataSourceUrl(mode, Some("mappings")),
      "dbtable" -> f"(SELECT userid, cast(yookoreid as text), username FROM legacyusers offset $cachedIndex%d) as legacyusers"
    )
  )

  val legacyfilesDF = sqlContext.load("jdbc", Map(
    "url" -> Config.dataSourceUrl(mode, Some("legacy")),
    "dbtable" -> "jiveattachment")
  )
  
  val df = mappingsDF.select(mappingsDF("userid"), 
    mappingsDF("yookoreid"), mappingsDF("username"))

  reduceFiles(df)
  
  def reduceFiles(mdf: DataFrame) = {
    mdf.collect().foreach(row => {
      cachedIndex = cachedIndex + 1
      cache.set("latest_legacy_files_index", cachedIndex.toString)
      val userid = row.getLong(0)
      upsert(row, userid)
    })
  }

  def upsert(row: Row, jiveuserid: Long) = {
    val attachment = legacyfilesDF.select(legacyfilesDF("attachmentid"), legacyfilesDF("objectid"), legacyfilesDF("objecttype"), legacyfilesDF("filename"), legacyfilesDF("filesize"), legacyfilesDF("contenttype"), legacyfilesDF("creationdate"), legacyfilesDF("modificationdate")).filter(f"objectid = $jiveuserid%d").collect()
      
    if (!attachment.isEmpty) {
      attachment.foreach {
        legacyfile =>
          // @todo Construct the binary filename as stored in the binstore
          // To find the specific directory of the file, 
          // we will need to take the first 3 characters of the file 
          // and turn those into 3 levels of directories.
          // For example, 500154tnemhcatta.bin
          println("===Retrieved attachmentid value=== " + legacyfile.getLong(0))
          val attachmentid = legacyfile.getLong(0)
          val binaryFilename = attachmentid + "54" + "tnemhcatta.bin"
          val fileid = java.util.UUID.randomUUID().toString()
          val jiveattachmentid = attachmentid
          val legacyjiveuserid = jiveuserid
          val userid = row.getString(1)
          val author = row.getString(2)
          val objectid = legacyfile.getLong(1)
          val objecttype = legacyfile.getInt(2)
          val filename = legacyfile.getString(3)
          val filesize = legacyfile.getInt(4)
          val contenttype = legacyfile.getString(5)
          val imageurl = Some(null)
          val creationdate = legacyfile.getLong(6)
          val modificationdate = legacyfile.getLong(7)
          sc.parallelize(Seq(JiveAttachment(
            fileid, jiveattachmentid, legacyjiveuserid, userid,
            author, objectid, objecttype, filename, binaryFilename,
            filesize, contenttype, imageurl, 
            creationdate, modificationdate )))
              .saveToCassandra(s"$keyspace", "legacyjiveattachment", SomeColumns("fileid", "jiveattachmentid", "jiveuserid", "userid",
                "author", "objectid", "objecttype",
                "filename", "binaryfilename", "filesize", "contenttype",
                "imageurl", "creationdate", "modificationdate"))
      }
    }
    
    println("===Latest JiveAttachment cachedIndex=== " + cache.get("latest_legacy_files_index").toInt)
  }

  def createSchema(conf: SparkConf): Boolean = {
    val keyspace = Config.cassandraConfig(mode, Some("keyspace"))
    val replicationStrategy = Config.cassandraConfig(mode, Some("replStrategy"))
    CassandraConnector(conf).withSessionDo { sess =>
      sess.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = $replicationStrategy")
      sess.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.legacyjiveattachment (fileid text, jiveattachmentid bigint, jiveuserid bigint, userid text, author text, objectid bigint, objecttype int, filename text, binaryfilename text, filesize int, contenttype text, imageurl text, creationdate bigint, modificationdate bigint, PRIMARY KEY ((fileid, userid), author)) WITH CLUSTERING ORDER BY (author DESC)")
    } wasApplied
  }
}
