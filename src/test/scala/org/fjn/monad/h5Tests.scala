package org.fjn.monad

import org.scalatest.junit.AssertionsForJUnit
import org.junit.Test

/**
 * Created by fran on 04/07/2014.
 */
class h5Tests extends AssertionsForJUnit{

//  @Test def openTest{
//
//    val obj = H5Monad("C:\\tmp\\test.h5")
//    obj.open
//    obj.close
//  }

  @Test def Create1LvlGroup{



    val obj: H5Object = H5Monad("C:\\tmp\\test.h5")


      var h = obj.create

      import H5Monad._


      val expected1 = Array(1.0,2.0,4.0)
      val expected2 = Array(1,2,4)
      val expected3 = "this is a string"

      obj in "/grp1/grp2/grp3" write(expected1,"dsxx0")
      obj in "/grp1/grp2/grp3" write(expected2,"dsxx1")
      obj in "/grp1/grp2/grp3" write(expected3.toByteArray,"dsStr1")

      obj in "/grp1/grp2/grp3" write(Array[Char]('a','b','c'),"dsChar1")
      obj.close

      h = obj.open
//
     val arr1:Array[Double] = obj from "/grp1/grp2/grp3" read "dsxx0"
     val arr2:Array[Int] = obj from "/grp1/grp2/grp3" read "dsxx1"
     val arr3:Array[Byte]= obj from "/grp1/grp2/grp3" read "dsStr1"


      obj.close

    assert((arr1 zip expected1).map(x =>  x._1 - x._2).foldLeft(0d)((a,b) => a  + b) == 0)
    assert((arr2 zip expected2).map(x =>  x._1 - x._2).foldLeft(0d)((a,b) => a  + b) == 0)
    val strA = arr3.fromArray2String
    assert(expected3 == strA)
  }

}
