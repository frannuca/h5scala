package org.fjn.shdf5

import ncsa.hdf.hdf5lib.{HDF5Constants, H5}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Created by fran on 05.07.2014.
 */

/**
 * Monadic Operation trait for writing and reading operation in hdf5 files
 * This trait is used as based for writing and reading derived trait. Two methods are injected from this trade depencing on the
 * operation being instanciated: read, write
 */
trait H5Ops{

  self:H5Ops =>


  import org.fjn.shdf5.CorrectPath._


  val obj:H5Id
  protected val locW:Option[String]


  /**
   * when applied set the location of the folder to be written and returns a writing monad object
   * @param location  path to the place where we want to write
   * @return H5MonadOpsWriter where to apply the method write
   */
  def in(location:String)=new H5OpsWriter{
    val locW = Some(location.toCorrectedH5Path)
    val obj = self.obj

  }

  /**
   * places the internal path reference to the place where to read data from
   * @param location  full path till the folder containing the dataset to be read
   * @return    H5OpsReader where to apply the method read
   */
  def from(location:String)=new H5OpsReader{
    val locW = Some(location.toCorrectedH5Path)
    val obj = self.obj
  }

}

/**
 * Reading operations from the hdf5 file. This trait specialized the general Operation trait with a new read method
 */
trait H5OpsReader extends H5Ops{


  def readAttribute[A:H5Transformation:ClassTag](datasetName:Option[String],attributeName:String):A = {

    val F = implicitly[H5Transformation[A]]
    val dataset_id =
      (locW,datasetName) match{
        case (Some(location),Some(dsName)) =>
          val ( _, ud )= obj.getDatasetId(location,dsName)
          ud
        case (Some(location),_) =>
          H5.H5Gopen(obj.fid,location)
        case _ => throw new Throwable("Invalid location provided to read attribute")

      }

    val attribute_id = H5.H5Aopen_by_name(dataset_id, ".", attributeName,
      HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);

    val dataspace_id = H5.H5Aget_space(attribute_id);

    H5.H5Sget_simple_extent_dims(dataspace_id, Array(1L), null)

    val dset_data = Array.ofDim[A](1)
    H5.H5Aread(attribute_id, F.getType, dset_data);



    H5.H5Aclose(attribute_id);
    H5.H5Dclose(dataset_id);
    H5.H5Sclose(dataspace_id);

    dset_data.headOption.getOrElse(throw new Throwable("invalid attribute"))
  }

  def readStringAttribute(datasetName:Option[String],attributeName:String):String = {


    import Using._


    val gid =
      (locW,datasetName) match{
        case (Some(location),Some(dsName)) =>
          val ( _, ud )= obj.getDatasetId(location,dsName)
          ud
        case (Some(location),_) =>
          H5.H5Gopen(obj.fid,location)
        case _ => throw new Throwable("Invalid location provided to read attribute")

      }


    val attribute_id = H5.H5Aopen_by_name(gid, ".", attributeName,HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT)
    val filetype_id = H5.H5Aget_type(attribute_id)
    val sdim = H5.H5Tget_size(filetype_id)+1

    val dataspace_id = H5.H5Aget_space(attribute_id)

    H5.H5Sget_simple_extent_dims(dataspace_id, Array(1L), null)
    val dset_data = Array.ofDim[Byte](1,sdim.toInt)
    val memtype_id = H5.H5Tcopy(HDF5Constants.H5T_C_S1)
    H5.H5Tset_size(memtype_id, sdim)

    H5.H5Aread(attribute_id, memtype_id, dset_data)
    //
    //
    //
    //
    H5.H5Aclose(attribute_id)
    //    if(datasetName.isDefined)
    H5.H5Dclose(gid)
    H5.H5Sclose(dataspace_id)

    dset_data.head.map(_.toChar).mkString

  }

  /**
   * Read simple array[_] stored in the given location
   * @param datasetName    name of set containing the data
   * @tparam A
   * @return    Array[_] of the elements stored in the dataset
   */
  def read[A:H5Transformation:ClassTag:Numeric](datasetName:String): Array[Array[A]] ={

    import Using._
    import H5Object._


    locW match{
      case Some(location)=>


        val F = implicitly[H5Transformation[A]]


        val typeInfo = F.getType
        val (group_id,dataset_id) = obj.getDatasetId(location,datasetName)

        val dspace  = H5.H5Dget_space(dataset_id)
        val ndims  = H5.H5Sget_simple_extent_ndims(dspace)

        val dims = Array.ofDim[Long](ndims)

        H5.H5Sget_simple_extent_dims(dspace, dims, null)


        val dset_data: Array[Array[A]] = Array.ofDim[A](dims(0).toInt,dims(1).toInt)

        try{
          H5.H5Dread(dataset_id,typeInfo,
            HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
            HDF5Constants.H5P_DEFAULT, dset_data)
          dset_data
        }
        catch{
          case e:Throwable => throw new Throwable(s"Exception while reading $location.",e)
        }


      case _=>
        throw new Throwable("location to read not set")
    }

  }

  def readString(datasetName:String): Array[String] ={

    import Using._
    import H5Object._


    locW match{
      case Some(location)=>


        val F = implicitly[H5Transformation[String]]


        val typeInfo = F.getType
        val (group_id,dataset_id) = obj.getDatasetId(location,datasetName)

        val filetype_id = H5.H5Dget_type(dataset_id);
        val sdim = H5.H5Tget_size(filetype_id)+1
        val dataspace_id = H5.H5Dget_space(dataset_id)



        val dim0 = readAttribute[Long](Some(datasetName),"size")
        H5.H5Sget_simple_extent_dims(dataspace_id, Array(dim0), null)
        val memtype_id = H5.H5Tcopy(HDF5Constants.H5T_C_S1);
        H5.H5Tset_size(memtype_id, sdim)


        val dset_data = Array.ofDim[Byte](dim0.toInt,sdim.toInt)

        H5.H5Dread(dataset_id, memtype_id,
          HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL, HDF5Constants.H5P_DEFAULT,
          dset_data);

        H5.H5Dclose(dataset_id);
        H5.H5Sclose(dataspace_id);
        H5.H5Tclose(filetype_id);
        H5.H5Tclose(memtype_id);

        dset_data.map(_.map(_.toChar).mkString.trim)

      case _=>
        throw new Throwable("location to read not set")
    }

  }

}

/**
 * Reading operations from the hdf5 file. This trait specialized the general Operation trait with a new write method
 */
trait H5OpsWriter extends H5Ops{

  self:H5Ops =>


  def writeAttr(datasetName:Option[String],attribute:(String,String)) ={

    val gid =
      (locW,datasetName) match{
        case (Some(location),Some(dsName)) =>
          val ( _, ud )= obj.getDatasetId(location,dsName)
          ud
        case (Some(location),_) =>
          H5.H5Gopen(obj.fid,location)
        case _ => throw new Throwable("Invalid location provided to read attribute")

      }

    writeAttribute(gid,attribute)
  }

  def writeAttr [A:H5Transformation:ClassTag:Numeric](path:String,attribute:(String,A)) ={

    val gid = obj.seek(path,false)

    writeAttribute[A](gid,attribute)
  }

  protected def writeAttribute [A:H5Transformation:ClassTag:Numeric](fid:Int,attribute:(String,A)): H5OpsWriter  ={

    import Using._


    val F = implicitly[H5Transformation[A]]

    val attr_dataspace_id = H5.H5Screate_simple(1, Array(1L), null)
    val attribute_id = H5.H5Acreate(fid, attribute._1, F.getType, attr_dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT)
    H5.H5Awrite(attribute_id, F.getType, Array(attribute._2))
    H5.H5Aclose(attribute_id)
    H5.H5Sclose(attr_dataspace_id)


    new H5OpsWriter {
      override protected val locW = None
      override val obj: H5Id = self.obj
    }

  }
  protected def writeAttribute(fid:Int,attribute:(String,String)): H5OpsWriter = {

    import Using._


    val F = implicitly[H5Transformation[String]]

    val SDIM = attribute._2.length

    val filetype_id = H5.H5Tcopy(HDF5Constants.H5T_FORTRAN_S1)
    H5.H5Tset_size(filetype_id,  SDIM -1 )

    val memtype_id = H5.H5Tcopy(HDF5Constants.H5T_C_S1)
    H5.H5Tset_size(memtype_id, SDIM)


    val dataspace_id = H5.H5Screate(HDF5Constants.H5S_SCALAR);

    val  dataset_id = fid



    val attr_dataspace_id = H5.H5Screate_simple(1, Array(1L), null)
    val attribute_id = H5.H5Acreate(dataset_id, attribute._1, filetype_id, attr_dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT)
    H5.H5Awrite(attribute_id, memtype_id, Array(attribute._2.toCharArray.map(_.toByte)))
    H5.H5Aclose(attribute_id)
    H5.H5Sclose(attr_dataspace_id)

    new H5OpsWriter {
      override protected val locW = None
      override val obj: H5Id = self.obj
    }

  }









  def writeString(a: Array[String], datasetname:String): H5OpsWriter ={

    locW match{
      case Some(location)=>

        val id = obj.seek(location,true)

        WriteDataSet2(a, datasetname, location)


        new H5OpsWriter {
          override protected val locW = None
          override val obj: H5Id = self.obj
        }

      case _=>
        throw new Throwable("no location specified")
    }


  }
  def write[A:H5Transformation:ClassTag:Numeric](a: Array[Array[A]], datasetname:String): H5OpsWriter ={

    import  Using._
    import H5Object._




    locW match{
      case Some(location)=>
        val F = implicitly[H5Transformation[A]]

        val typeInfo = F.getType


        val id = obj.seek(location,true)

        WriteDataSet(a, datasetname, location, typeInfo)




        new H5OpsWriter {
          override protected val locW = None
          override val obj: H5Id = self.obj
        }

      case _=>
        throw new Throwable("no location specified")
    }
  }



  //TODO: try to include 2D Data modify the DataSpacemanager to be used (dimX,dimY)
  private def WriteDataSet[A: H5Transformation : ClassTag]
      (a: Array[Array[A]], datasetName: String, location: String, h5TypeConverted: Int) {
    import Using._
    using(DataSpaceManager(Array(a.size.toLong, a.headOption.map(_.length.toLong).getOrElse(0L)))) { dsp => {
      using(DataSetManagerCreator(obj.fid, location, datasetName, dsp.id, h5TypeConverted)) { dset => {

        H5.H5Dwrite(dset.dataset_id, h5TypeConverted,
          HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
          HDF5Constants.H5P_DEFAULT, a)




      }

      }
    }
    }
  }

  private def WriteDataSet2(a: Array[String], datasetName: String, location: String) {
    import Using._


    val DIM0 = a.length.toLong
    val SDIM = a.map(_.length).max.toLong+1


    val fileTypeId = H5.H5Tcopy(HDF5Constants.H5T_FORTRAN_S1)
    H5.H5Tset_size(fileTypeId, SDIM-1)
    val memTypeId = H5.H5Tcopy(HDF5Constants.H5T_C_S1)
    H5.H5Tset_size(memTypeId, SDIM)

    try{

      using(DataSpaceManager(Array(DIM0))) { dsp => {
        using(DataSetManagerCreator(obj.fid, location, datasetName, dsp.id, fileTypeId)) { dset => {


          val data = a.map(_.padTo(SDIM.toInt,'\0')).map(_.toCharArray.map(_.toByte))

          H5.H5Dwrite(dset.dataset_id, memTypeId,
            HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
            HDF5Constants.H5P_DEFAULT, data)


          writeAttribute(dset.dataset_id,("size",DIM0))



        }

        }
      }
      }
    }
    finally{
      H5.H5Tclose(fileTypeId);
      H5.H5Tclose(memTypeId);

    }


  }
}