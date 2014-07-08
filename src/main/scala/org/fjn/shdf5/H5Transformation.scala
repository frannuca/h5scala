package org.fjn.shdf5

import ncsa.hdf.hdf5lib.{H5, HDF5Constants}


/**
 * transformation from unsupported types which can be transferred as Byte. One case woudl be Char, which can be automatically
 * transferred ad Byte for reading and writing
 * @tparam A
 */

trait H5Transformation[A] {
  def getType: Int
}

object H5Transformation {

  /** enabling implicits on Optional, allowing to translate missing expected implicit object to None **/
  implicit def OptionalImplicit[A <: AnyRef](implicit a: A = null) = Option(a)




  implicit val h5IntTrans = new H5Transformation[Int] {
    def getType: Int = HDF5Constants.H5T_NATIVE_INT
  }


  /** ****************************************************************************************
    * List of transformations for simple types Char, Double, Float etc...
    * ********************************************************************************************/

  implicit val h5DoubleTrans = new H5Transformation[Double] {
    def getType: Int = HDF5Constants.H5T_NATIVE_DOUBLE
  }

  implicit val h5FloatType = new H5Transformation[Float] {
    def getType: Int = HDF5Constants.H5T_NATIVE_FLOAT
  }



  implicit val h5LType = new H5Transformation[Long] {
    def getType: Int = HDF5Constants.H5T_NATIVE_INT64
  }

  implicit val h5ShortType = new H5Transformation[Short] {
    def getType: Int = HDF5Constants.H5T_NATIVE_SHORT
  }

  implicit val h5StringType = new H5Transformation[String] {
    def getType: Int = HDF5Constants.H5T_FORTRAN_S1
  }

  implicit val h5SType = new H5Transformation[Byte] {
    def getType: Int = H5.H5Tcopy(HDF5Constants.H5T_NATIVE_B8)
  }

}
