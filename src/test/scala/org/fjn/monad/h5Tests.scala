package org.fjn.monad

import org.junit.runners.MethodSorters
import org.scalatest.junit.AssertionsForJUnit
import org.junit.{FixMethodOrder, Test}

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

    val obj: H5Object = H5Object("C:\\tmp\\test.h5")


      var h = obj.create

      import H5Object._


      val expected1 = Array(1.0,2.0,4.0,121.09)
      val expected2 = Array(1,2,4,9999)
      val expected3 = "this is a string for all"
      val expected4=Array('a','b','c')

      obj in "/grp1/grp2/grp3/" write(expected1,"dsxx0")
      obj in "/grp1/grp2/grp3" write(expected2,"dsxx1")
      obj in "/grp1/grp2/A" write(expected3.toCharArray.map(_.toByte),"dsStr1")

      obj in "/grp1/grp2/grp3" write(expected4,"dsChar1")
      obj.close

      h = obj.open
//
     val arr1:Array[Double] = obj from "/grp1/grp2/grp3" read "dsxx0"
     val arr2:Array[Int] = obj from "/grp1/grp2/grp3" read "dsxx1"
     val arr3:Array[Byte]= obj from "/grp1/grp2/A" read "dsStr1"
     val arr4:Array[Char]= obj from "/grp1/grp2/grp3" read "dsChar1"


      obj.close

    assert((arr1 zip expected1).map(x =>  x._1 - x._2).foldLeft(0d)((a,b) => a  + b) == 0)
    assert((arr2 zip expected2).map(x =>  x._1 - x._2).foldLeft(0d)((a,b) => a  + b) == 0)
    val strA = arr3.map(_.toChar).mkString
    assert(expected3 == strA)

    assert(arr4.mkString == expected4.mkString)
  }


}
