package org.fjn.shdf5

import java.io.File


import org.apache.log4j.Logger
import org.junit.runner.RunWith
import org.junit.Assert.{assertEquals, assertArrayEquals}
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers



//TODO: expose writing attributes as Array({name:value}) cover long,double, string
//TODO: do array of string as 1D first and if time 2D matrices
/**
 * Created by fran on 05.07.2014.
 */
@RunWith(classOf[JUnitRunner])
class TestWritingReading  extends FlatSpec with ShouldMatchers{

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

  val arrayOfDouble =  Array(Array.ofDim[Double](1000).transform(_ => rngGen.nextDouble()).toArray)
  val arrayOfFloat  =  Array(Array.ofDim[Float](1000).transform(_ => rngGen.nextFloat()) .toArray)
  val arrayOfInt   =  Array(Array.ofDim[Int](1000).transform(_ => rngGen.nextInt(Int.MaxValue)) .toArray)
  val arrayOfLong   =  Array(Array.ofDim[Long](1000).transform(_ => rngGen.nextLong()) .toArray)
  val arrayOfBytes0   = Array.ofDim[Byte](1000); rngGen.nextBytes(arrayOfBytes0);
  val arrayOfBytes = Array(arrayOfBytes0)
  val arrayOfStrings = Array("This is the first","This is the second","And the last but not least the third--->>>>>Yes")





  "The test should" should "write and read data for" in {



       logger.info(s"Starting test for open and close hdf5 file $tempFilePath")

       val obj: H5Object = H5Object(tempFilePath)


       logger.info("creating new file ...")
       val h = obj.create
       logger.info(s"new file created with root id ${h.fid} ")


       logger.info("writing and array of strings")
    val a: Array[Array[String]] = Array(arrayOfStrings)
       obj in "/test/strings" writeGroupAttribute (("mygroupLongAttr1",121L),("mygroupStringAttr1","small step for man, huge leap for humanitz"))
       obj in "/test/strings" write(a,"data",("myDouble",32.9),("myString","Meaning of life and universe is 42"))

       obj in "/test/general/arrayof/" writeGroupAttribute(("Groupd attr1",3897.0))

       logger.info("writing array of doubles ...")
       val arrayof = obj in "/test/general/arrayof/"


//
       logger.info("writing array of doubles ...")
    arrayof.write(arrayOfDouble, "dsDoubles",("the meaning of life and universe",42L),("The Pi as float",3.14f),("The Pi as double",3.14d))

    logger.info("writing array of floats ...")
    arrayof.write(arrayOfFloat, "dsFloats")


    logger.info("writing array of Int ...")
       arrayof.write(arrayOfInt, "dsIntegers")

       logger.info("writing array of Long ...")
       arrayof.write(arrayOfLong, "dsLongs")


       logger.info("writing array of Bytes ...")
       arrayof.write(arrayOfBytes, "dsBytes")



       logger.info("closing writing operations")
       obj.close


       logger.info("opening read only  operations")
       obj.open


      logger.info("reading array of strings")
      val arrayOfStrings2 = obj from "test/strings" readString("data")
       logger.info("reading array of doubles ...")

       val arrayOfDouble2:Array[Array[Double]] = obj from "/test/general/arrayof/" read("dsDoubles")
//
       logger.info("reading array of floats ...")
       val arrayOfFloat2:Array[Array[Float]] = obj from "/test/general/arrayof/" read "dsFloats"

       logger.info("reading array of Int ...")
       val arrayOfInt2: Array[Array[Int]] = obj from "/test/general/arrayof/" read "dsIntegers"

       logger.info("reading array of Longs ...")
       val arrayOfLong2: Array[Array[Long]] = obj from "/test/general/arrayof/" read "dsLongs"

//
       logger.info("writing array of Bytes ...")
       val arrayOfBytes2: Array[Array[Byte]] = obj from "/test/general/arrayof/" read "dsBytes"


    logger.info("getting attributes")
    val attr1 = obj.from("/test/general/arrayof/").readAttribute[Double](None,"Groupd attr1")


    val attr2 = obj.from("/test/general/arrayof/").readAttribute[Float](Some("dsDoubles"),"The Pi as float")
       logger.info("comparing data ...")


    val attr3 =  obj.from("/test/strings").readAttribute[String](Some("data"),"myString")

      assertEquals("Failed to recover array of Doubles", (arrayOfDouble.head zip arrayOfDouble2.head).map(x => x._1 - x._2).fold(0.0d)(_ + _), 0.0d, 0.0d)
      assertEquals("Failed to recover array of Floats", (arrayOfFloat.head zip arrayOfFloat2.head).map(x => x._1 - x._2).fold(0.0f)(_ + _), 0.0f, 0.0f)
      assertEquals("Failed to recover array of Integers", (arrayOfInt.head zip arrayOfInt2.head).map(x => x._1 - x._2).fold(0)(_ + _), 0)
      assertEquals("Failed to recover array of Long", (arrayOfLong.head zip arrayOfLong2.head).map(x => x._1 - x._2).fold(0L)(_ + _), 0)
      assertArrayEquals("Failed to recover array of Bytes", arrayOfBytes.head, arrayOfBytes2.head)

       logger.info("success!!!")

       logger.info("closing reading operations")
       obj.close

  }

}
