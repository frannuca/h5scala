package org.fjn.monad

import ncsa.hdf.hdf5lib.{HDF5Constants, H5}

/**
 * Created by fran on 04/07/2014.
 */
case class DataSetManagerCreator(file_id:Int,path0:String,datasetName:String,dataspace_id:Int,typeId:Int){
  import CorrectPath._

  var path = path0.toCorrectedH5Path

  val dataset_id = H5.H5Dcreate(file_id,
     path + datasetName,  typeId,
    dataspace_id, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);

  def close(){
    H5.H5Dclose(dataset_id)
  }
}
