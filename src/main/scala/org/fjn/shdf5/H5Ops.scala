package org.fjn.shdf5

import ncsa.hdf.hdf5lib.{HDF5Constants, H5}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag}
import  scala.reflect.runtime.{universe => ru}

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


  def readAttribute[A:H5Transformation:ClassTag:ru.TypeTag](datasetName:Option[String],attributeName:String):A = {

    if(ru.typeOf[A] =:= ru.typeOf[String]){
      readStringAttribute(datasetName,attributeName).asInstanceOf[A]
    } else{
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
      if(datasetName.isDefined)
        H5.H5Dclose(dataset_id);
      else
        H5.H5Gclose(dataset_id);

      H5.H5Sclose(dataspace_id);

      dset_data.headOption.getOrElse(throw new Throwable("invalid attribute"))
    }

  }

  private def readStringAttribute(datasetName:Option[String],attributeName:String):String = {


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
    if(datasetName.isDefined)
      H5.H5Dclose(gid)
    else
      H5.H5Gclose(gid)

    H5.H5Sclose(dataspace_id)

    dset_data.head.map(_.toChar).mkString.trim

  }

  /**
   * Read simple array[_] stored in the given location
   * @param datasetName    name of set containing the data
   * @tparam A
   * @return    Array[_] of the elements stored in the dataset
   */
  def read[A:H5Transformation:ClassTag:TypeTag:Numeric](datasetName:String): Array[Array[A]] ={

    {
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



  def writeGroupAttribute[AT1:H5Transformation:ClassTag:TypeTag](attr1:(String,AT1))= {

    val id: Int = locW match{
      case Some(location)=>


        obj.seek(location,true)((ig)=>{
          writeAttribute[AT1](ig,attr1)
        })

      case _=>
        throw new Throwable("no location specified")
    }


    ()

  }
  def writeGroupAttribute[AT1:H5Transformation:ClassTag:TypeTag,
                          AT2:H5Transformation:ClassTag:TypeTag](attr1:(String,AT1),attr2:(String,AT2))= {

    val id: Int = locW match{
      case Some(location)=>


        obj.seek(location,true)((ig)=>{
          writeAttribute[AT1](ig,attr1)
          writeAttribute[AT2](ig,attr2)
        })

      case _=>
        throw new Throwable("no location specified")
    }


    ()

  }

  def writeGroupAttribute[AT1:H5Transformation:ClassTag:TypeTag,
                          AT2:H5Transformation:ClassTag:TypeTag,
                          AT3:H5Transformation:ClassTag:TypeTag](attr1:(String,AT1),attr2:(String,AT2),attr3:(String,AT3))= {

    val id: Int = locW match{
      case Some(location)=>


        obj.seek(location,true)((ig)=>{
          writeAttribute[AT1](ig,attr1)
          writeAttribute[AT2](ig,attr2)
          writeAttribute[AT3](ig,attr3)
        })

      case _=>
        throw new Throwable("no location specified")
    }


    ()

  }
  protected def writeAttribute [A:H5Transformation:ClassTag:TypeTag](fid:Int,attribute:(String,A)): H5OpsWriter  ={

    import Using._


    if(ru.typeOf[A] =:= ru.typeOf[String]){
      writeAttribute4String(fid,(attribute._1,attribute._2.toString) )
    }
    else{
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






  }
  protected def writeAttribute4String(fid:Int,attribute:(String,String)): H5OpsWriter = {

    import Using._


    val F = implicitly[H5Transformation[String]]

    val SDIM = attribute._2.length+1

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

  private def writeString(a: Array[String], datasetname:String)(fds:Int=>Unit): H5OpsWriter ={

    locW match{
      case Some(location)=>

        val id = obj.seek(location,true)(_=>())
        WriteDataSet2(a, datasetname, location)(fds)



        new H5OpsWriter {
          override protected val locW = None
          override val obj: H5Id = self.obj
        }

      case _=>
        throw new Throwable("no location specified")
    }


  }
  def write[A:H5Transformation:ClassTag:TypeTag](a: Array[Array[A]], datasetname:String): H5OpsWriter={

    ru.typeOf[A] match{
      case t if t =:= ru.typeOf[String] =>     writeString(a.head.map(_.toString), datasetname)(_=>())
      case t => writeBase(a, datasetname)(_=>())
    }
  }

  def write[A:H5Transformation:ClassTag:TypeTag,AT1:H5Transformation:ClassTag:TypeTag](a: Array[Array[A]], datasetname:String,attr1:(String,AT1)): H5OpsWriter={

    val fds = (sid:Int)=>{ writeAttribute[AT1](sid,attr1);() }


    ru.typeOf[A] match{
      case t if t =:= ru.typeOf[String] =>     writeString(a.head.map(_.toString), datasetname)(fds)
      case t => writeBase(a, datasetname)(fds)
    }
  }
  def write[A:H5Transformation:ClassTag:TypeTag,AT1:H5Transformation:ClassTag:TypeTag,AT2:H5Transformation:ClassTag:TypeTag](a: Array[Array[A]], datasetname:String,attr1:(String,AT1),attr2:(String,AT2)): H5OpsWriter={

    val fds = (sid:Int)=>{
      writeAttribute[AT1](sid,attr1)
      writeAttribute[AT2](sid,attr2)
      () }


    ru.typeOf[A] match{
      case t if t =:= ru.typeOf[String] =>     writeString(a.head.map(_.toString), datasetname)(fds)
      case t => writeBase(a, datasetname)(fds)
    }
  }

  def write[A:H5Transformation:ClassTag:TypeTag,
            AT1:H5Transformation:ClassTag:TypeTag,
            AT2:H5Transformation:ClassTag:TypeTag,
            AT3:H5Transformation:ClassTag:TypeTag](a: Array[Array[A]], datasetname:String,attr1:(String,AT1),attr2:(String,AT2),attr3:(String,AT3)): H5OpsWriter={

    val fds = (sid:Int)=>{
      writeAttribute[AT1](sid,attr1)
      writeAttribute[AT2](sid,attr2)
      writeAttribute[AT3](sid,attr3)
      () }


    ru.typeOf[A] match{
      case t if t =:= ru.typeOf[String] =>     writeString(a.head.map(_.toString), datasetname)(fds)
      case t => writeBase(a, datasetname)(fds)
    }
  }

  def write[A:H5Transformation:ClassTag:TypeTag,
  AT1:H5Transformation:ClassTag:ru.TypeTag,
  AT2:H5Transformation:ClassTag:ru.TypeTag,
  AT3:H5Transformation:ClassTag:ru.TypeTag,
  AT4:H5Transformation:ClassTag:ru.TypeTag](a: Array[Array[A]], datasetname:String,attr1:(String,AT1),attr2:(String,AT2),attr3:(String,AT3),attr4:(String,AT4)): H5OpsWriter={


    val fds = (sid:Int)=>{
      writeAttribute[AT1](sid,attr1)
      writeAttribute[AT2](sid,attr2)
      writeAttribute[AT3](sid,attr3)
      writeAttribute[AT4](sid,attr4)
      () }


    ru.typeOf[A] match{
      case t if t =:= ru.typeOf[String] =>     writeString(a.head.map(_.toString), datasetname)(fds)
      case t => writeBase(a, datasetname)(fds)
    }
  }

  private def writeBase[A:H5Transformation:ClassTag:TypeTag](a: Array[Array[A]], datasetname:String)(fds:Int=>Unit): H5OpsWriter ={

    import  Using._
    import H5Object._




    locW match{
      case Some(location)=>
        val F = implicitly[H5Transformation[A]]

        val typeInfo = F.getType


        val id = obj.seek(location,true)(_=>())

        WriteDataSet(a, datasetname, location, typeInfo)(fds)




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
      (a: Array[Array[A]], datasetName: String, location: String, h5TypeConverted: Int)(fds:Int=>Unit) {
    import Using._
    using(DataSpaceManager(Array(a.size.toLong, a.headOption.map(_.length.toLong).getOrElse(0L)))) { dsp => {
      using(DataSetManagerCreator(obj.fid, location, datasetName, dsp.id, h5TypeConverted)) { dset => {

        H5.H5Dwrite(dset.dataset_id, h5TypeConverted,
          HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
          HDF5Constants.H5P_DEFAULT, a)


        fds(dset.dataset_id)

      }

      }
    }
    }
  }

  private def WriteDataSet2(a: Array[String], datasetName: String, location: String)(fds:Int=>Unit) {
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

          fds(dset.dataset_id)
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