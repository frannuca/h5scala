package org.fjn.monad

import ncsa.hdf.hdf5lib.H5
import ncsa.hdf.hdf5lib.HDF5Constants
import scala.StringContext.InvalidEscapeException
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag





  trait H5MapTransformation[A]{
    def map(a:A):Byte
    def imap(b:Byte):A
    val typeInfo = HDF5Constants.H5T_NATIVE_B8
  }

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

    import  Using._
    import H5ObjectTransformations._
    val  mapping = implicitly[Option[H5MapTransformation[A]]]

    locW match{
      case Some(Left(location))=>
        val F = implicitly[H5MonadType[A]]
        val typeInfo = F.getType
        val m = implicitly[ClassTag[A]]





        val id = obj.seek(location,true)


        using(DataSpaceManager(a.size,1)){dsp =>{
          using(DataSetManagerCreator(obj.fid,location,datasetName,dsp.id,typeInfo)){ dset =>{



           mapping match{
             case Some(m)=>
               H5.H5Dwrite(dset.dataset_id,  mapping.map(_.typeInfo).getOrElse(typeInfo),
                 HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                 HDF5Constants.H5P_DEFAULT, a.map(d=> m.map(d)))
             case None =>
               H5.H5Dwrite(dset.dataset_id,  mapping.map(_.typeInfo).getOrElse(typeInfo),
                 HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                 HDF5Constants.H5P_DEFAULT, a)

           }


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

  def read[A:H5MonadType:ClassTag](datasetName:String): Array[A] ={

    import Using._
    import H5ObjectTransformations._
    val  mapping = implicitly[Option[H5MapTransformation[A]]]

    locW match{
      case Some(Right(location))=>


        val F = implicitly[H5MonadType[A]]
        val typeInfo = F.getType
        val dataset_id = obj.getDatasetId(location,datasetName)

        val dspace  = H5.H5Dget_space(dataset_id)
        val ndims  = H5.H5Sget_simple_extent_ndims(dspace)

        val dims = Array[Long](ndims)

        H5.H5Sget_simple_extent_dims(dspace, dims, null)

        val dset_data = mapping.map(t => Array.ofDim[Byte](dims(0).toInt)).getOrElse( Array.ofDim[A](dims(0).toInt))

        H5.H5Dread(dataset_id,mapping.map(_.typeInfo).getOrElse(typeInfo),
          HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
          HDF5Constants.H5P_DEFAULT, dset_data)


        mapping.map(_.typeInfo).getOrElse(typeInfo) match{
          case HDF5Constants.H5T_NATIVE_B8 => dset_data.map(_.asInstanceOf[Byte]).map( mapping.get.imap _)
          case _=>    dset_data.map(_.asInstanceOf[A])

        }

      case _=>
        throw new Throwable("location to read not set")
    }

}



}


trait H5MonadType[A] {
  def getType:Int
}



