package com.yookos.migration

import akka.actor.{ Actor, Props, ActorSystem, ActorRef }
import akka.pattern.{ ask, pipe }
import akka.event.Logging
import akka.util.Timeout

import org.apache.spark._
import org.apache.spark.sql._

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
  
  protected def reduceFiles(df: DataFrame) = {
    df.collect().foreach(row => {
      cachedIndex = cachedIndex + 1
      cache.set("latest_legacy_files_index", cachedIndex.toString)
      val userid = row.getLong(0)
      upsert(row, userid)
    })
  }

  private def upsert(row: Row, jiveuserid: Long) = {
    legacyfilesDF.select(legacyfilesDF("attachmentid"), legacyfilesDF("objectid"), legacyfilesDF("objecttype"), legacyfilesDF("filename"), legacyfilesDF("filesize"), legacyfilesDF("contenttype"), legacyfilesDF("creationdate"), legacyfilesDF("modificationdate")).filter(f"objectid = $jiveuserid%d").foreach {

    legacyfile =>
      // @todo Construct the binary filename as stored in the binstore
      // To find the specific directory of the file, 
      // we will need to take the first 3 characters of the file 
      // and turn those into 3 levels of directories.
      // For example, 500154tnemhcatta.bin
      val attachmentid = legacyfile.getLong(0)
      val binaryFilename = attachmentid + "54" + "tnemhcatta.bin"
      sc.parallelize(Seq(Tuple14(
        java.util.UUID.randomUUID().toString(),
        attachmentid,
        row.getLong(0),
        row.getString(1),
        row.getString(2),
        legacyfile.getLong(1),
        legacyfile.getLong(2),
        legacyfile.getString(3),
        binaryFilename,
        legacyfile.getLong(4),
        legacyfile.getString(5),
        Some(null),
        legacyfile.getLong(6),
        legacyfile.getLong(7))))
          .saveToCassandra(s"$keyspace", "legacyjiveattachment", SomeColumns("fileid, jiveattachmentid", "jiveuserid", "userid",
            "author", "objectid", "objecttype",
            "filename", "binaryfilename", "filesize", "contenttype",
            "imageurl", "creationdate", "modificationdate"))
    }
        
    println("===Latest JiveAttachment cachedIndex=== " + cache.get("latest_legacy_files_index").toInt)
  }

  def createSchema(conf: SparkConf): Boolean = {
    val keyspace = Config.cassandraConfig(mode, Some("keyspace"))
    val replicationStrategy = Config.cassandraConfig(mode, Some("replStrategy"))
    CassandraConnector(conf).withSessionDo { sess =>
      sess.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = $replicationStrategy")
      sess.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.legacyjiveattachment (fileid text, jiveattachmentid bigint, jiveuserid bigint, userid text, author text, objectid bigint, objecttype bigint, filename text, binaryfilename text, filesize bigint, contenttype text, imageurl text, creationdate bigint, modificationdate bigint, PRIMARY KEY ((fileid, userid), author)) WITH CLUSTERING ORDER BY (author DESC)")
    } wasApplied
  }
}
