package org.fjn.shdf5

import java.io.File


import org.apache.log4j.Logger
import org.junit.runner.RunWith
import org.junit.Assert.{assertEquals, assertArrayEquals}
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import scala.reflect.ClassTag
import scala.reflect.runtime._


/**
 * Created by fran on 05.07.2014.
 */
@RunWith(classOf[JUnitRunner])
class TestWritingReading  extends FlatSpec with org.scalatest.Matchers{

  val logger = Logger.getLogger(classOf[TestWritingReading].getName())


  lazy val tempFilePath = {
    val tempFile = File.createTempFile(java.util.UUID.randomUUID().toString, ".h5")
    val path = tempFile.getAbsolutePath()

    logger.info("path="+path)
    tempFile.delete()
    path

  }


  val rngGen = new java.util.Random()

  val arrayOfArrayDouble: Array[Array[Double]] =  (for(i <- 0 until 21) yield {
      Array.ofDim[Double](1500).transform(_ => rngGen.nextDouble()).toArray
  }).toArray


  val array3DofDoubles: Array[Array[Array[Double]]] =  (for(i <- 0 until 2) yield {
    (for(j <- 0 until 21) yield{
      Array.ofDim[Double](1500).transform(_ => rngGen.nextDouble()).toArray
    }).toArray
  }).toArray

  import RandomMatrixGenerators._

  val arrayOfDouble =  createMatrixOf[Double](300,3000)
  val arrayOfFloat  =  createMatrixOf[Float](1,300)
  val arrayOfInt   =   createMatrixOf[Int](150,300)
  val arrayOfLong   =  createMatrixOf[Long](200,300)
  val arrayOfBytes   = createMatrixOf[Byte](200,300)
  val arrayOfStrings = Array(Array("This is the first","This is the second","And the last but not least the third--->>>>>Yes"))
  val attrd = 1.0d
  val attrf = 2.0f
  val attrstr= "XXX"
  val attrL = 1L
  val attrI = 3





  "The test should" should "write and read data for" in {



       logger.info(s"Starting test for open and close hdf5 file $tempFilePath")

       val obj: H5Object = H5Object(tempFilePath)


       logger.info("creating new file ...")

       val h = obj.create
       logger.info(s"new file created with root id ${h.fid} ")


       logger.info("writing and array of strings")




    def writeHelper(place:String,dsStr:String)={
      obj.ingroup(place)
        .withDataSet(dsStr)
        .withDatasetAttribute("attrd",attrf)
        .withDatasetAttribute("attrf",attrf)
        .withDatasetAttribute("attrstr",attrstr)
        .withDatasetAttribute("attrL",attrL)
        .withDatasetAttribute("attrI",attrI)
    }


    writeHelper("/test/arrayOfStrings","ds1")
      .withGroupAttribute("attrd",attrf)
      .withGroupAttribute("attrf",attrf)
      .withGroupAttribute("attrstr",attrstr)
      .withGroupAttribute("attrL",attrL)
      .withGroupAttribute("attrI",attrI).write[String](arrayOfStrings)

    writeHelper("/test/numeric","dsD")
      .withGroupAttribute("attrd",attrd)
      .withGroupAttribute("attrf",attrf)
      .withGroupAttribute("attrstr",attrstr)
      .withGroupAttribute("attrL",attrL)
      .withGroupAttribute("attrI",attrI).write[Double](arrayOfDouble)

    writeHelper("/test/numeric","dsF").write[Float](arrayOfFloat)

    writeHelper("/test/numeric","dsL").write[Long](arrayOfLong)

    writeHelper("/test/numeric","dsB").write[Byte](arrayOfBytes)

    obj.close




    def readHelperAttribute[A:H5Transformation:ClassTag:universe.TypeTag](place:String,dsStr:String,attrName:String)={
      obj.fromgroup(place).readDataAttribute[A](dsStr, "attrd")
    }


    obj.open

    {
      val arrayOfStrings2 = obj.fromgroup("/test/arrayOfStrings").readString("ds1")
      val attrf_ = obj.fromgroup("/test/arrayOfStrings").readDataAttribute[Float]("ds1", "attrf")
      val attrstr_ = obj.fromgroup("/test/arrayOfStrings").readDataAttribute[String]("ds1", "attrstr")
      val attrL_ = obj.fromgroup("/test/arrayOfStrings").readDataAttribute[Long]("ds1", "attrL")
      val attrI_ = obj.fromgroup("/test/arrayOfStrings").readDataAttribute[Int]("ds1", "attrI")
      assertEquals("Failed to recover array of Strings", (arrayOfStrings.head zip arrayOfStrings2).map(x => if (x._1 != x._2) 1.0 else 0.0).fold(0.0d)(_ + _), 0.0d, 0.0d)
    }
    {
      val arrayOfDouble2 = obj.fromgroup("/test/numeric").readNumeric[Double]("dsD")
      val attrd_ = obj.fromgroup("/test/numeric").readDataAttribute[Double]("dsD", "attrd")
      val attrf_ = obj.fromgroup("/test/numeric").readDataAttribute[Float]("dsD", "attrf")
      val attrstr_ = obj.fromgroup("/test/numeric").readDataAttribute[String]("dsD", "attrstr")
      val attrL_ = obj.fromgroup("/test/numeric").readDataAttribute[Long]("dsD", "attrL")
      val attrI_ = obj.fromgroup("/test/numeric").readDataAttribute[Int]("dsD", "attrI")
      assertEquals("Failed to recover array of doubles", (arrayOfDouble.head zip arrayOfDouble2.head).map(x => x._1 - x._2).fold(0.0d)(_ + _), 0.0d, 0.0d)
    }

    {
      val arrayOfFloat2 = obj.fromgroup("/test/numeric").readNumeric[Float]("dsF")
      val attrd_ = obj.fromgroup("/test/numeric").readDataAttribute[Double]("dsF", "attrd")
      val attrf_ = obj.fromgroup("/test/numeric").readDataAttribute[Float]("dsF", "attrf")
      val attrstr_ = obj.fromgroup("/test/numeric").readDataAttribute[String]("dsF", "attrstr")
      val attrL_ = obj.fromgroup("/test/numeric").readDataAttribute[Long]("dsF", "attrL")
      val attrI_ = obj.fromgroup("/test/numeric").readDataAttribute[Int]("dsF", "attrI")
      assertEquals("Failed to recover array of floats", (arrayOfFloat.head zip arrayOfFloat2.head).map(x => x._1 - x._2).fold(0.0f)(_ + _), 0.0d, 0.0d)
    }
    {
      val arrayOfLong2 = obj.fromgroup("/test/numeric").readNumeric[Long]("dsL")
      val attrd_ = obj.fromgroup("/test/numeric").readDataAttribute[Double]("dsL", "attrd")
      val attrf_ = obj.fromgroup("/test/numeric").readDataAttribute[Float]("dsL", "attrf")
      val attrstr_ = obj.fromgroup("/test/numeric").readDataAttribute[String]("dsL", "attrstr")
      val attrL_ = obj.fromgroup("/test/numeric").readDataAttribute[Long]("dsL", "attrL")
      val attrI_ = obj.fromgroup("/test/numeric").readDataAttribute[Int]("dsL", "attrI")
      assertEquals("Failed to recover array of floats", (arrayOfLong.head zip arrayOfLong2.head).map(x => x._1 - x._2).fold(0L)(_ + _), 0, 0)
    }

    {
      val arrayOfBytes2 = obj.fromgroup("/test/numeric").readNumeric[Byte]("dsB")
      //assertEquals("Failed to recover array of floats", (arrayOfBytes2.head zip arrayOfBytes2.head).map(x => x._1 - x._2).fold(0L)(_ + _), 0, 0)
      val aux1d=obj.fromgroup("/test/numeric").readGroupAttribute[Double]( "attrd")
      assertEquals("comparing attribute double",aux1d,attrd,0)
      assertEquals("comparing attribute double",obj.fromgroup("/test/numeric").readGroupAttribute[Float]("attrf"),attrf,0)
      assertEquals("comparing attribute string",obj.fromgroup("/test/numeric").readGroupAttribute[String]( "attrstr"),attrstr)
      assertEquals("comparing attribute Long",obj.fromgroup("/test/numeric").readGroupAttribute[Long]("attrL") ,attrL)
      assertEquals("Comparing attribute Int",obj.fromgroup("/test/numeric").readGroupAttribute[Int]( "attrI"), attrI)
      assertEquals("Failed to recover array of floats", (arrayOfBytes.head zip arrayOfBytes2.head).map(x => x._1.toInt - x._2.toInt).fold(0)(_ + _), 0, 0)
    }


    obj.close

    obj.openRW

    {
      obj.ingroup("/test/strings2")
        .withDataSet("ds1")
        .withDatasetAttribute("attrd",attrf)
        .withDatasetAttribute("attrf",attrf)
        .withDatasetAttribute("attrstr",attrstr)
        .withDatasetAttribute("attrL",attrL)
        .withDatasetAttribute("attrI",attrI)
        .withGroupAttribute("attrd",attrf)
        .withGroupAttribute("attrf",attrf)
        .withGroupAttribute("attrstr",attrstr)
        .withGroupAttribute("attrL",attrL)
        .withGroupAttribute("attrI",attrI)
        .write(arrayOfStrings)
    }

    obj.close

//      assertEquals("Failed to recover array of Doubles", (arrayOfDouble.head zip arrayOfDouble2.head).map(x => x._1 - x._2).fold(0.0d)(_ + _), 0.0d, 0.0d)
//      assertEquals("Failed to recover array of Floats", (arrayOfFloat.head zip arrayOfFloat2.head).map(x => x._1 - x._2).fold(0.0f)(_ + _), 0.0f, 0.0f)
//      assertEquals("Failed to recover array of Integers", (arrayOfInt.head zip arrayOfInt2.head).map(x => x._1 - x._2).fold(0)(_ + _), 0)
//      assertEquals("Failed to recover array of Long", (arrayOfLong.head zip arrayOfLong2.head).map(x => x._1 - x._2).fold(0L)(_ + _), 0)
//      assertArrayEquals("Failed to recover array of Bytes", arrayOfBytes.head, arrayOfBytes2.head)
//
//       logger.info("success!!!")
//
//       logger.info("closing reading operations")
//       obj.close
//
//
//
//      obj.openRW
//      obj ingroup "/test/secondTime" writedataset(a,"data",("myDouble",32.9),("myString","Meaning of life and universe is 42"))
//      obj.close



  }

}

