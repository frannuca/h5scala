package org.fjn.monad

import ncsa.hdf.hdf5lib.{H5, HDF5Constants}

/**
 * Created by fran on 04/07/2014.
 */

/**
 * Abstract H5 object container managing the create,open and close actions on HDF5 files
 */
trait H5Object {

  /** file path of the hdf5 */
  val filePath: String

  /** * internal file id. This field is mutable since the user can open and close the file multiple times */
  private var fileId: Option[Int] = None


  /**
   * Truncate file, if it already exists, erasing all data previously stored in the file or creates a new one
   * @return   a simple H5Id instance containing the internal h5 internal file description for the root node
   */
  def create = {
    if (fileId.isEmpty)
      fileId = Some(H5.H5Fcreate(filePath, HDF5Constants.H5F_ACC_TRUNC,
        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT))


    new H5Id {
      val fid = fileId.getOrElse(-1)
    }
  }

  /**
   * Allow read-only access to file.
   * @return   a simple H5Id instance containing the internal h5 internal file description for the root node
   */
  def open = {
    if (fileId.isEmpty)
      fileId = Some(H5.H5Fopen(filePath, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT))


    new H5Id {
      val fid = fileId.getOrElse(-1)
    }
  }

  /**
   * Closes the file and removes the internal file descriptor is have been set before.
   * It is allowed to reopen or re-create the file once it has been closed
   */
  def close {
    fileId.foreach(H5.H5Fclose(_))
    fileId = None
  }

}


/**
 * Companion object containing all implicits H5MapTransformation.
 */
object H5Object {

  /**
   * given a path returns the correspondent H5Object
   * @param path   h5 file location
   * @return   H5Object instance
   */
  def apply(path: String): H5Object = {
    new H5Object {
      val filePath = path

    }
  }


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


  /** ****************************************************************************************
    * List of transformations for simple types Char, Double, Float etc...
    * ********************************************************************************************/

  implicit val h5IntTrans = new H5MonadType[Int] {
    def getType: Int = HDF5Constants.H5T_NATIVE_INT
    override val mapping = implicitly[Option[H5MapTransformation[Int]]]
  }

  implicit val h5DoubleTrans = new H5MonadType[Double] {
    def getType: Int = HDF5Constants.H5T_NATIVE_DOUBLE
    override val mapping = implicitly[Option[H5MapTransformation[Double]]]
  }

  implicit val h5FloatType = new H5MonadType[Float] {
    def getType: Int = HDF5Constants.H5T_NATIVE_FLOAT
    override val mapping = implicitly[Option[H5MapTransformation[Float]]]
  }

  implicit val h5CType = new H5MonadType[Char] {
    def getType: Int = HDF5Constants.H5T_NATIVE_CHAR

    override val mapping = implicitly[Option[H5MapTransformation[Char]]]
  }


  implicit val h5LType = new H5MonadType[Long] {
    def getType: Int = HDF5Constants.H5T_NATIVE_LONG

    override val mapping = implicitly[Option[H5MapTransformation[Long]]]
  }

  implicit val h5ShortType = new H5MonadType[Short] {
    def getType: Int = HDF5Constants.H5T_NATIVE_SHORT

    override val mapping = implicitly[Option[H5MapTransformation[Short]]]
  }


  implicit val h5SType = new H5MonadType[Byte] {
    def getType: Int = H5.H5Tcopy(HDF5Constants.H5T_NATIVE_B8)

    override val mapping = implicitly[Option[H5MapTransformation[Byte]]]
  }


  /**
   * operation injection of write and read method for H5 instances
   * @param x
   * @return
   */
  implicit def H5Mondad2Typed(x: H5Object): H5MonadOps = {

    new H5MonadOps {
      override val obj: H5Id = x.open
      override val locW = None
    }
  }

}