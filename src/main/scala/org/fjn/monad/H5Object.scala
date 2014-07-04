package org.fjn.monad

import ncsa.hdf.hdf5lib.{HDF5Constants, H5}

/**
 * Created by fran on 04/07/2014.
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
