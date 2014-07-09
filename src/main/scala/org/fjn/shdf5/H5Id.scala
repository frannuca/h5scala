package org.fjn.shdf5

import ncsa.hdf.hdf5lib.{HDF5Constants, H5}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/**
 * Created by fran on 04/07/2014.
 */

/**
 * Provides the functionality to navigate and create groups withing the underneath H5Object
 */
trait H5Id{
  val fid:Int

  /**
   * Places the cursor into the provided position craating on the demand any group that maybe specified in the path.
   * @param path0  location inside the HDF5 file where to place the cursor
   * @param createOntheFly if set to true any group non existent in the file but specified in the path will be created
   * @return return the internal identifier of the provided position
   */
  def seek(path0:String,createOntheFly:Boolean)(fgroupAttribute:(Int)=>Unit): Int ={

    val path = if(path0.last == '/')
                  path0.init
               else
                  path0

    @tailrec def seekInternal(baseId:Int,path:List[String],ids:ListBuffer[Int],partialPath:ListBuffer[String]):Int={
      path match{
        case head::x =>
          val id =
            if(H5.H5Lexists(baseId,head,HDF5Constants.H5P_DEFAULT) || !createOntheFly)
              H5.H5Gopen(baseId,head,HDF5Constants.H5P_DEFAULT)
            else
              H5.H5Gcreate(baseId, head,HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT)


          ids += id
          seekInternal(id,x,ids,partialPath)

        case Nil =>  baseId
      }
    }


    val ids = new ListBuffer[Int]()
    val partialPath = new ListBuffer[String]
    val gid = seekInternal(fid,path.split("/").filter(!_.isEmpty).toList,ids,partialPath)

    fgroupAttribute(gid)
    ids.reverse.foreach(q =>
      H5.H5Gclose(q)
    )
    gid
  }


  /**
   * given the path to a given group and a dataset name inside the group it returns the associate internal id of the dataset
   * @param path group path with '/' separators. i.e: "/series/SWX"
   * @param datasetName name of the dataset. i.e "window1"
   * @return  tuple with (group id, dataset id)
   */
  def getDatasetId(path:String,datasetName:String)={

    val gid = H5.H5Gopen(fid,path,HDF5Constants.H5P_DEFAULT)

    (gid,H5.H5Dopen(gid,datasetName,HDF5Constants.H5P_DEFAULT))

  }

}
