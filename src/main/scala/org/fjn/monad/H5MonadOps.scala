package org.fjn.monad

import ncsa.hdf.hdf5lib.{HDF5Constants, H5}

import scala.reflect.ClassTag

/**
 * Created by fran on 05.07.2014.
 */

/**
 * Monadic Operation trait for writing and reading operation in hdf5 files
 * This trait is used as based for writing and reading derived trait. Two methods are injected from this trade depencing on the
 * operation being instanciated: read, write
 */
trait H5MonadOps{

  self:H5MonadOps =>


  import org.fjn.monad.CorrectPath._


  val obj:H5Id
  protected val locW:Option[String]


  /**
   * when applied set the location of the folder to be written and returns a writing monad object
   * @param location  path to the place where we want to write
   * @return H5MonadOpsWriter where to apply the method write
   */
  def in(location:String)=new H5MonadOpsWriter{
    val locW = Some(location.toCorrectedH5Path)
    val obj = self.obj

  }

  /**
   * places the internal path reference to the place where to read data from
   * @param location  full path till the folder containing the dataset to be read
   * @return    H5MonadOpsReader where to apply the method read
   */
  def from(location:String)=new H5MonadOpsReader{
    val locW = Some(location.toCorrectedH5Path)
    val obj = self.obj
  }

}

/**
 * Reading operations from the hdf5 file. This trait specialized the general Operation trait with a new read method
 */
trait H5MonadOpsReader extends H5MonadOps{

  def read[A:H5Monad:ClassTag](datasetName:String): Array[A] ={

    import Using._
    import H5Object._


    locW match{
      case Some(location)=>


        val F = implicitly[H5Monad[A]]
        val  mapping = F.mapping

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

/**
 * Reading operations from the hdf5 file. This trait specialized the general Operation trait with a new write method
 */
trait H5MonadOpsWriter extends H5MonadOps{

  self:H5MonadOps =>
  def write[A:H5Monad:ClassTag](a: Array[A],datasetName:String) ={

    import  Using._
    import H5Object._


    locW match{
      case Some(location)=>
        val F = implicitly[H5Monad[A]]
        val  mapping = F.mapping

        val typeInfo = F.getType
        val h5TypeConverted =  mapping.map(_.typeInfo).getOrElse(typeInfo)
        val m = implicitly[ClassTag[A]]





        val id = obj.seek(location,true)


        using(DataSpaceManager(a.size,1)){dsp =>{
          using(DataSetManagerCreator(obj.fid,location,datasetName,dsp.id,h5TypeConverted)){ dset =>{



            mapping match{
              case Some(m)=>
                H5.H5Dwrite(dset.dataset_id, h5TypeConverted,
                  HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                  HDF5Constants.H5P_DEFAULT, a.map(d=> m.map(d).asInstanceOf[Byte]))
              case None =>
                H5.H5Dwrite(dset.dataset_id,  h5TypeConverted,
                  HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                  HDF5Constants.H5P_DEFAULT, a)

            }


          }

          }
        }}

        new H5MonadOpsWriter {
          override protected val locW = None
          override val obj: H5Id = self.obj
        }

      case _=>
        throw new Throwable("no location specified")
    }

  }

}