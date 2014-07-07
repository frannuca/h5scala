package org.fjn.shdf5

import java.io.File

import org.apache.log4j.Logger
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers



/**
 * Created by fran on 05.07.2014.
 */
@RunWith(classOf[JUnitRunner])
class TestJUnitOpenClose extends FlatSpec with ShouldMatchers {

  val logger = Logger.getLogger(classOf[TestJUnitOpenClose].getName());


  lazy val tempFilePath = {
    val tempFile = File.createTempFile("xxxx", ".h5")
    val path = tempFile.getAbsolutePath()
    tempFile.delete()
    path

  }


  "The test should" should "open and close" in {


    logger.info(s"Starting test for open and close hdf5 file $tempFilePath")



    val obj: H5Object = H5Object(tempFilePath)


    logger.info("creating new file ...")
    val h = obj.create
    logger.info(s"new file created with root id ${h.fid} ")
    logger.info("closing the file")
    obj.close
    logger.info("reopening the file")
    val h2 = obj.open
    logger.info(s"file reopened with root id ${h2.fid}")


    logger.info("closing and leaving the test")
    obj.close

  }

}
