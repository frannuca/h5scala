package org.fjn.shdf5
import ncsa.hdf.hdf5lib.{HDF5Constants, H5}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag}
import  scala.reflect.runtime.{universe => ru}
/**
 * Reading operations from the hdf5 file. This trait specialized the general Operation trait with a new write method
 */
trait H5OpsWriter extends H5Ops{

  self:H5Ops =>


  protected var attributeFunctions4Datasets:Function1[Int,Int]=identity[Int]
  protected var attributeFunctions4Groups:Function1[Int,Int] = identity[Int]

  protected var datasetname:Option[String] = None


  def withDatasetAttribute[AT1:H5Transformation:ClassTag:TypeTag](name:String,value:AT1)={

    def f(sid:Int)={

      writeAttribute[AT1](sid,(name,value))
      sid
    }


    attributeFunctions4Datasets = attributeFunctions4Datasets andThen f
    this
  }


  def withGroupAttribute[AT1:H5Transformation:ClassTag:TypeTag](name:String,value:AT1)={


    def f(sid:Int)={

      writeGroupAttribute[AT1]((name,value))
      sid
    }


    attributeFunctions4Groups = attributeFunctions4Groups andThen f
    this
  }



  def withDataSet(datasetName:String)={
    datasetname = Some(datasetName)

    this
  }



  protected def writeGroupAttribute[AT1:H5Transformation:ClassTag:TypeTag](attr1:(String,AT1))= {

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


  protected def writeAttribute [A:H5Transformation:ClassTag:TypeTag](fid:Int,attribute:(String,A)){


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

    }
  }
  protected def writeAttribute4String(fid:Int,attribute:(String,String)) {


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

  }

  private def writeString(a: Array[String]){

    (locW,datasetname) match{
      case (Some(location),Some(dataset))=>

        val id = obj.seek(location,true)(_=>())
        WriteDataSet2(a, dataset, location)(id =>{attributeFunctions4Datasets(id); ()})


      case _=>
        throw new Throwable("no location specified")
    }


  }


  def write[A:H5Transformation:ClassTag:TypeTag](a: Array[Array[A]]){


    attributeFunctions4Groups(0)
    if(!a.isEmpty)
      ru.typeOf[A] match{
        case t if t =:= ru.typeOf[String] =>     writeString(a.head.map(_.toString))
        case t => writeBase(a)
      }
  }

  private def writeBase[A:H5Transformation:ClassTag:TypeTag](a: Array[Array[A]]){

    (locW,datasetname) match{
      case (Some(location),Some(dataset))=>
        val F = implicitly[H5Transformation[A]]

        val typeInfo = F.getType


        val id = obj.seek(location,true)(_=>())

        WriteDataSet(a, dataset, location, typeInfo)((x:Int)=>{attributeFunctions4Datasets(x);()})



      case _=>
        throw new Throwable("no location specified")
    }
  }

  private def WriteDataSet[A: H5Transformation : ClassTag]
      (a: Array[Array[A]], datasetName: String, location: String, h5TypeConverted: Int)(fds:Int=>Unit) {
    import org.fjn.shdf5.Using._
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
    import org.fjn.shdf5.Using._


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
