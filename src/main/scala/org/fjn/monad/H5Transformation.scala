package org.fjn.monad

import ncsa.hdf.hdf5lib.{H5, HDF5Constants}


/**
 * transformation from unsupported types which can be transferred as Byte. One case woudl be Char, which can be automatically
 * transferred ad Byte for reading and writing
 * @tparam A
 */
trait H5MapTransformation[A] {
  def map(a: A): Byte

  def imap(b: Byte): A

  def typeInfo = HDF5Constants.H5T_NATIVE_B8
}


trait H5Transformation[A] {
  def getType: Int

  val mapping: Option[H5MapTransformation[A]]
}


object H5Transformation {

  /** enabling implicits on Optional, allowing to translate missing expected implicit object to None **/
  implicit def OptionalImplicit[A <: AnyRef](implicit a: A = null) = Option(a)


  /**
   * map transformation necessary to allow simple translation non-numeric type into bytes
   * This operation is defined to transfer into the hdf5 Chars as Bytes.
   */
  implicit val h5CharTrans = new H5MapTransformation[Char] {
    override def map(a: Char): Byte = a.toByte

    override def imap(b: Byte): Char = b.toChar
  }


  implicit val h5IntTrans = new H5Transformation[Int] {
    def getType: Int = HDF5Constants.H5T_NATIVE_INT

    override val mapping = implicitly[Option[H5MapTransformation[Int]]]
  }


  /** ****************************************************************************************
    * List of transformations for simple types Char, Double, Float etc...
    * ********************************************************************************************/

  implicit val h5DoubleTrans = new H5Transformation[Double] {
    def getType: Int = HDF5Constants.H5T_NATIVE_DOUBLE

    override val mapping = implicitly[Option[H5MapTransformation[Double]]]
  }

  implicit val h5FloatType = new H5Transformation[Float] {
    def getType: Int = HDF5Constants.H5T_NATIVE_FLOAT

    override val mapping = implicitly[Option[H5MapTransformation[Float]]]
  }

  implicit val h5CType = new H5Transformation[Char] {
    def getType: Int = HDF5Constants.H5T_NATIVE_CHAR

    override val mapping = implicitly[Option[H5MapTransformation[Char]]]
  }

  implicit val h5LType = new H5Transformation[Long] {
    def getType: Int = HDF5Constants.H5T_NATIVE_INT64

    override val mapping = implicitly[Option[H5MapTransformation[Long]]]
  }

  implicit val h5ShortType = new H5Transformation[Short] {
    def getType: Int = HDF5Constants.H5T_NATIVE_SHORT

    override val mapping = implicitly[Option[H5MapTransformation[Short]]]
  }


  implicit val h5SType = new H5Transformation[Byte] {
    def getType: Int = H5.H5Tcopy(HDF5Constants.H5T_NATIVE_B8)

    override val mapping = implicitly[Option[H5MapTransformation[Byte]]]
  }

}
