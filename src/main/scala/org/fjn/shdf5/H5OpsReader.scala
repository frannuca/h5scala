package org.fjn.shdf5
import ncsa.hdf.hdf5lib.{HDF5Constants, H5}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag}
import  scala.reflect.runtime.{universe => ru}
/**
 * Reading operations from the hdf5 file. This trait specialized the general Operation trait with a new read method
 */
trait H5OpsReader extends H5Ops{


  def readDataAttribute[A:H5Transformation:ClassTag:ru.TypeTag](datasetname:String,attributeName:String):A={
       readAttribute(Some(datasetname),attributeName)
  }

  def readGroupAttribute[A:H5Transformation:ClassTag:ru.TypeTag](datasetname:String,attributeName:String):A={
    readAttribute(None,attributeName)
  }


  protected def readAttribute[A:H5Transformation:ClassTag:ru.TypeTag](datasetName:Option[String],attributeName:String):A = {

    if(ru.typeOf[A] =:= ru.typeOf[String]){
      readStringAttribute(datasetName,attributeName).asInstanceOf[A]
    } else {

      val F = implicitly[H5Transformation[A]]
      val (group_id,dataset_id) =
        (locW,datasetName) match{
          case (Some(location),Some(dsName)) =>
            obj.getDatasetId(location,dsName)

          case (Some(location),_) =>
            (-1,H5.H5Gopen(obj.fid,location,HDF5Constants.H5P_DEFAULT))
          case _ => throw new Throwable("Invalid location provided to read attribute")

        }

      val attribute_id = H5.H5Aopen_by_name(dataset_id, ".", attributeName,
        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);

      val dataspace_id = H5.H5Aget_space(attribute_id);

      H5.H5Sget_simple_extent_dims(dataspace_id, Array(1L), null)

      val dset_data = Array.ofDim[A](1)
      H5.H5Aread(attribute_id, F.getType, dset_data);



      H5.H5Aclose(attribute_id)
      if(datasetName.isDefined)
        H5.H5Dclose(dataset_id)
      else
        H5.H5Gclose(dataset_id)

      H5.H5Sclose(dataspace_id)

      if(group_id>0)
        H5.H5Gclose(group_id)


      dset_data.headOption.getOrElse(throw new Throwable("invalid attribute"))
    }

  }

  private def readStringAttribute(datasetName:Option[String],attributeName:String):String = {


    val (group_id,gid) =
      (locW,datasetName) match{
        case (Some(location),Some(dsName)) =>
          obj.getDatasetId(location,dsName)
        case (Some(location),_) =>
          (-1,H5.H5Gopen(obj.fid,location,HDF5Constants.H5P_DEFAULT))
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
    if(datasetName.isDefined)
      H5.H5Dclose(gid)
    else
      H5.H5Gclose(gid)

    H5.H5Sclose(dataspace_id)

    if(group_id>0)
      H5.H5Gclose(group_id)

    dset_data.head.map(_.toChar).mkString.trim

  }

  /**
   * Read simple array[_] stored in the given location
   * @param datasetName    name of set containing the data
   * @tparam A
   * @return    Array[_] of the elements stored in the dataset
   */
  def readNumeric[A:H5Transformation:ClassTag:TypeTag:Numeric](datasetName:String): Array[Array[A]] ={

    {


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

            H5.H5Dclose(dataset_id)
            H5.H5Gclose(group_id)

            dset_data
          }
          catch{
            case e:Throwable => throw new Throwable(s"Exception while reading $location.",e)
          }


        case _=>
          throw new Throwable("location to read not set")
      }
    }


  }

  def readString(datasetName:String): Array[String] ={


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

        H5.H5Dclose(dataset_id)
        H5.H5Gclose(group_id)
        H5.H5Sclose(dataspace_id)
        H5.H5Tclose(filetype_id)
        H5.H5Tclose(memtype_id)

        dset_data.map(_.map(_.toChar).mkString.trim)

      case _=>
        throw new Throwable("location to read not set")
    }

  }

}
