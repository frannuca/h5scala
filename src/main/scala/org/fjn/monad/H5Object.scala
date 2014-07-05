package org.fjn.monad

import ncsa.hdf.hdf5lib.{HDF5Constants, H5}

/**
 * Created by fran on 04/07/2014.
 */

/**
 * purely type free H5 container. Use this class to  create, open and close hdf5 files.
 * It contains an internal H5Id instance carrying the file root id along. This instance is used on other functional
 * traits to perform access to the hdf5 data.
 * @param filePath path to the location of the h5 file
 */
case class H5Object(filePath:String){
  private var fileId:Option[Int] = None


  def create = {
    if(fileId.isEmpty)
      fileId = Some(H5.H5Fcreate(filePath, HDF5Constants.H5F_ACC_TRUNC,
        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT))


    new H5Id {
      val fid = fileId.getOrElse(-1)
    }
  }

  def open = {
    if(fileId.isEmpty)
      fileId = Some(H5.H5Fopen(filePath,HDF5Constants.H5F_ACC_RDONLY,HDF5Constants.H5P_DEFAULT))


    new H5Id {
      val fid = fileId.getOrElse(-1)
    }
  }

  def close{
      fileId.foreach(H5.H5Fclose(_))
      fileId=None
  }

}


object H5ObjectTransformations{

  /**
   * given a path returs the correspondent H5Object
   * @param path   h5 file location
   * @return   H5Object instance
   */
  def apply(path:String): H5Object ={
    H5Object(filePath = path)
  }


  implicit def OptionalImplicit[A <: AnyRef](implicit a: A = null) = Option(a)


  implicit def CharTransformation = new H5MapTransformation[Char]{
    override def map(a: Char): Byte = a.toByte

    override def imap(b: Byte): Char = b.toChar
  }

  implicit val h5IType = new  H5MonadType[Int] {
    def getType: Int = HDF5Constants.H5T_NATIVE_INT
  }

  implicit val h5DType = new  H5MonadType[Double]{
    def getType:Int = HDF5Constants.H5T_NATIVE_DOUBLE
  }

  implicit val h5FType = new  H5MonadType[Float]{
    def getType:Int = HDF5Constants.H5T_NATIVE_FLOAT
  }

  implicit val h5CType = new  H5MonadType[Char]{
    def getType:Int = HDF5Constants.H5T_NATIVE_B8
  }

  implicit val h5StringType = new  H5MonadType[String]{
    def getType:Int = HDF5Constants.H5T_NATIVE_B8
  }

  implicit val h5LType = new  H5MonadType[Long]{
    def getType:Int = HDF5Constants.H5T_NATIVE_LONG
  }

  implicit val h5ShortType = new  H5MonadType[Short]{
    def getType:Int = HDF5Constants.H5T_NATIVE_SHORT
  }


  implicit val h5SType = new  H5MonadType[Byte]{
    def getType:Int = H5.H5Tcopy(HDF5Constants.H5T_NATIVE_B8)
  }


  implicit def FromByteArray2Str(s:Array[Byte])= new{
    def fromArray2String: String = s.map(_.toChar).mkString
  }

  implicit def H5Mondad2Typed(x:H5Object):H5MonadOps={

    new H5MonadOps{
      override val  obj:H5Id = x.open
      override val locW=None
    }
  }

}