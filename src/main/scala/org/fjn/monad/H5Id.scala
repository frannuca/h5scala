package org.fjn.monad

import ncsa.hdf.hdf5lib.{HDF5Constants, H5}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/**
 * Created by fran on 04/07/2014.
 */
trait H5Id{
  val fid:Int
  /**
   * position the cursor to the specified group position.
   * @param path path locating the group to be reached. i.e: /mygroup1/mygroup2
   * @return id of the location given in path
   */
  def seek(path0:String,createOntheFly:Boolean)={

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

    ids.reverse.foreach(q =>
      H5.H5Gclose(q)
    )
    gid
  }


  def getDatasetId(path:String,datasetName:String)={


    val gid = H5.H5Gopen(fid,path)

    H5.H5Dopen(gid,datasetName)
  }

}
