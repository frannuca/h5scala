package org.fjn.shdf5

import ncsa.hdf.hdf5lib.{HDF5Constants, H5}

/**
 * Created by fran on 04/07/2014.
 */

/**
 * Helper class to open a dataset at the provided path with the given name
 * @param file_id internal id number from which the path starts to be specified
 * @param path0 folder path from the file_id location till the dataset location
 * @param datasetName name of the dataset node
 * @param dataspace_id dataspace id. This object is ncessary to be passed in order to compute space allocation.
 *                     ser DataSpaceManager for more information
 * @param typeId    hdf5 type id expression the nature of the data to be inserted
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
