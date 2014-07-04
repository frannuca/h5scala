package org.fjn.monad

import ncsa.hdf.hdf5lib.H5
import ncsa.hdf.hdf5lib.HDF5Constants
import scala.StringContext.InvalidEscapeException
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag












trait H5MonadOps{

  self:H5MonadOps =>

  val obj:H5Id
  protected val locW:Option[Either[String,String]]


  def in(location:String)=new H5MonadOps{
    val locW = Some(Left(location))
    val obj = self.obj
  }

  def from(location:String)=new H5MonadOps{
    val locW = Some(Right(location))
    val obj = self.obj
  }
  
  def write[A:H5MonadType:ClassTag](a: Array[A],datasetName:String) ={

    locW match{
      case Some(Left(location))=>
        val typeInfo = implicitly[H5MonadType[A]].getType
        val m = implicitly[ClassTag[A]]
        //    val a: Array[A] =
        //      if(m.isInstanceOf[H5MonadTypeVAR[A]]){
        //        val m2: H5MonadTypeVAR[A] = m.asInstanceOf[H5MonadTypeVAR[A]]
        //
        //      Array(a0.foldLeft(m2.mzero)((a,b)=>m2.plus(a,b)))
        //    }
        //    else
        //        a0



        import  Using._

        val id = obj.seek(location,true)


        using(DataSpaceManager(a.size,1)){dsp =>{
          using(DataSetManagerCreator(obj.fid,location,datasetName,dsp.id,typeInfo)){ dset =>{

            H5.H5Dwrite(dset.dataset_id, typeInfo,
              HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
              HDF5Constants.H5P_DEFAULT,a)
          }

          }
        }}

        new H5MonadOps {
          override protected val locW = None
          override val obj: H5Id = self.obj
        }

      case _=>
        throw new Throwable("no location specified")
    }

  }

  def read[A:H5MonadType:ClassTag](datasetName:String) : Array[A] ={

    locW match{
      case Some(Right(location))=>
        import Using._
        val typeInfo = implicitly[H5MonadType[A]].getType
        val dataset_id = obj.getDatasetId(location,datasetName)

        val dspace  = H5.H5Dget_space(dataset_id)
        val ndims  = H5.H5Sget_simple_extent_ndims(dspace)

        val dims = Array[Long](ndims)

        H5.H5Sget_simple_extent_dims(dspace, dims, null);

        val dset_data = Array.ofDim[A](dims(0).toInt)


        H5.H5Dread(dataset_id, typeInfo,
          HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
          HDF5Constants.H5P_DEFAULT, dset_data)


        dset_data

      case _=>
        throw new Throwable("location to read not set")
    }

}




}

trait H5MonadTypeVAR[A] extends H5MonadType[A] {
 def mzero:A
 def plus(a:A,b:A):A
}

trait H5MonadType[A] {
  def getType:Int
}

//trait H5Monad[A] {
//  val h5:H5Id
//  implicit val typeMonad:H5MonadType[A]
//  def getType:Int = typeMonad.getType
//}




object H5Monad{

  def apply(path:String): H5Object ={
    H5Object(filePath = path)
  }


  implicit val h5IType = new  H5MonadType[Int]{
    def getType:Int = HDF5Constants.H5T_NATIVE_INT
  }

  implicit val h5DType = new  H5MonadType[Double]{
    def getType:Int = HDF5Constants.H5T_NATIVE_DOUBLE
  }

  implicit val h5FType = new  H5MonadType[Float]{
    def getType:Int = HDF5Constants.H5T_NATIVE_FLOAT
  }

  implicit val h5CType = new  H5MonadType[Char]{
    def getType:Int = HDF5Constants.H5T_NATIVE_SHORT
  }

  implicit val h5LType = new  H5MonadType[Long]{
    def getType:Int = HDF5Constants.H5T_NATIVE_LONG
  }

  implicit val h5ShortType = new  H5MonadType[Short]{
    def getType:Int = HDF5Constants.H5T_NATIVE_SHORT
  }


  implicit val h5SType = new  H5MonadType[Byte]{
    def getType:Int = H5.H5Tcopy(HDF5Constants.H5T_NATIVE_B8)
    def plus(a:String,b:String)= a + b
    def mzero = ""
  }

  implicit def FromString2ByteArray(s:String)= new{
    def toByteArray: Array[Byte] = s.toCharArray.map(_.toByte)
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