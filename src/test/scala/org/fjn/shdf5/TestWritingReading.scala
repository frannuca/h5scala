package org.fjn.shdf5

import java.io.File


import org.apache.log4j.Logger
import org.junit.Test
import org.junit.Assert.{assertEquals, assertArrayEquals}
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}

/**
 * Created by fran on 05.07.2014.
 */

class TestWritingReading  extends AssertionsForJUnit {

  val logger = Logger.getLogger(classOf[TestWritingReading].getName());


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

  val arrayOfDouble: Array[Double] =  Array.ofDim[Double](1000).transform(_ => rngGen.nextDouble()).toArray
  val arrayOfFloat:  Array[Float]  =  Array.ofDim[Float](1000).transform(_ => rngGen.nextFloat()) .toArray
  val arrayOfInt:    Array[Int]    =  Array.ofDim[Int](1000).transform(_ => rngGen.nextInt(Int.MaxValue)) .toArray
  val arrayOfLong:   Array[Long]   =  Array.ofDim[Long](1000).transform(_ => rngGen.nextLong()) .toArray
  val arrayOfChar:   Array[Char]   =  Array.ofDim[Char](1000).transform(_ => 'a').toArray
  val arrayOfBytes:  Array[Byte]   =  Array.ofDim[Byte](1000); rngGen.nextBytes(arrayOfBytes)





   @Test def readWriteTypes {


     try {
       logger.info(s"Starting test for open and close hdf5 file $tempFilePath")

       val obj: H5Object = H5Object(tempFilePath)


       logger.info("creating new file ...")
       val h = obj.create
       logger.info(s"new file created with root id ${h.fid} ")



       logger.info("writing array of array ...")
       obj in "test/arrayofarray" write(arrayOfArrayDouble, "data")


       logger.info("writing a 3d matrix ...")
       obj in "test/array3DofDouble" write(array3DofDoubles, "data")

       logger.info("writing array of doubles ...")
       obj in "/test/general/arrayof/" write(arrayOfDouble, "dsDoubles")

       logger.info("writing array of floats ...")
       obj in "/test/general/arrayof/" write(arrayOfFloat, "dsFloats")

       logger.info("writing array of Int ...")
       obj in "/test/general/arrayof/" write(arrayOfInt, "dsIntegers")

       logger.info("writing array of Long ...")
       obj in "/test/general/arrayof/" write(arrayOfLong, "dsLongs")

       logger.info("writing array of Char ...")
       obj in "/test/general/arrayof/" write(arrayOfChar, "dsChars")

       logger.info("writing array of Bytes ...")
       obj in "/test/general/arrayof/" write(arrayOfBytes, "dsBytes")

       logger.info("closing writing operations")
       obj.close


       logger.info("opening read only  operations")
       obj.open


       logger.info("reading array of array of doubles ...")
       val arrayOfArrayDouble2: Array[Array[Double]] = obj from "test/arrayofarray" read2DMatrix "data"


       logger.info("reading 3d matrix of doubles ...")
       val array3DofDoubles2: Array[Array[Array[Double]]] = obj from "test/array3DofDouble" read3DMatrix "data"

       logger.info("reading array of doubles ...")
       val arrayOfDouble2: Array[Double] = obj from "/test/general/arrayof/" read "dsDoubles"

       logger.info("reading array of floats ...")
       val arrayOfFloat2: Array[Float] = obj from "/test/general/arrayof/" read "dsFloats"

       logger.info("reading array of Int ...")
       val arrayOfInt2: Array[Int] = obj from "/test/general/arrayof/" read "dsIntegers"

       logger.info("reading array of Int ...")
       val arrayOfLong2: Array[Long] = obj from "/test/general/arrayof/" read "dsLongs"

       logger.info("writing array of Char ...")
       val arrayOfChar2: Array[Char] = obj from "/test/general/arrayof/" read "dsChars"

       logger.info("writing array of Bytes ...")
       val arrayOfBytes2: Array[Byte] = obj from "/test/general/arrayof/" read "dsBytes"


       logger.info("comparing data ...")



       arrayOfArrayDouble.indices.foreach(i => {

         assertEquals("Failed to recover array of Doubles", (arrayOfArrayDouble(i) zip arrayOfArrayDouble2(i)).map(x => x._1 - x._2).fold(0.0d)(_ + _), 0.0d, 0.0d)
       })

       array3DofDoubles.indices.foreach(i => {
         array3DofDoubles(i).indices.foreach(j => {
           logger.info(s"doing $i , $j ")
           assertEquals("Failed to recover array of Doubles", (array3DofDoubles(i)(j) zip array3DofDoubles2(i)(j)).map(x => x._1 - x._2).fold(0.0d)(_ + _), 0.0d, 0.0d)
         })
       })

       assertEquals("Failed to recover array of Doubles", (arrayOfDouble zip arrayOfDouble2).map(x => x._1 - x._2).fold(0.0d)(_ + _), 0.0d, 0.0d)
       assertEquals("Failed to recover array of Floats", (arrayOfFloat zip arrayOfFloat2).map(x => x._1 - x._2).fold(0.0f)(_ + _), 0.0f, 0.0f)
       assertEquals("Failed to recover array of Integers", (arrayOfInt zip arrayOfInt2).map(x => x._1 - x._2).fold(0)(_ + _), 0)
       assertEquals("Failed to recover array of Long", (arrayOfLong zip arrayOfLong2).map(x => x._1 - x._2).fold(0L)(_ + _), 0)
       assertEquals("Failed to recover array of Char", (arrayOfChar zip arrayOfChar2).map(x => x._1.toInt - x._2.toInt).fold(0)(_ + _), 0)
       assertArrayEquals("Failed to recover array of Bytes", arrayOfBytes, arrayOfBytes2)

       logger.info("success!!!")

       logger.info("closing reading operations")
       obj.close
     }
     catch{
       case e=> logger.error(e.getStackTraceString); throw e
     }
  }

}
