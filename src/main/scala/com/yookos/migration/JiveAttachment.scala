package com.yookos.migration;

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.mapper._

case class JiveAttachment(
    fileid: String, 
    jiveattachmentid: Long, 
    jiveuserid: Long, 
    userid: String, 
    author: String, 
    objectid: Long, 
    objecttype: Int, 
    filename: String, 
    binaryfilename: String, 
    filesize: Int, 
    contenttype: String, 
    imageurl: Option[String],
    creationdate: Long, 
    modificationdate: Long 
  ) extends Serializable

object JiveAttachment {
  implicit object Mapper extends DefaultColumnMapper[JiveAttachment](
    Map("fileid" -> "fileid",
      "jiveattachmentid" -> "jiveattachmentid",
      "userid" -> "userid",
      "author" -> "author",
      "jiveuserid" -> "jiveuserid",
      "objectid" -> "objectid",
      "objecttype" -> "objecttype",
      "creationdate" -> "creationdate",
      "modificationdate" -> "modificationdate",
      "filename" -> "filename",
      "binaryfilename" -> "binaryfilename",
      "filesize" -> "filesize",
      "contenttype" -> "contenttype",
      "imageurl" -> "imageurl"
      )  
  )

}
