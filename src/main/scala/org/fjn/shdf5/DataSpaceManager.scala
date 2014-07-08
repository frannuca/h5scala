package org.fjn.shdf5

import ncsa.hdf.hdf5lib.H5

/**
 * Created by fran on 04/07/2014.
 */

/**
 * Helper class to define the space and type allocation of the data to be transferred

 */
case class DataSpaceManager(dims:Array[Long]){

  val id = H5.H5Screate_simple(dims.length,dims,null)

  def close(){
    H5.H5Sclose(id)
    }
}
case class DataSpaceManager4String(dims:Array[Long]){

  val id = H5.H5Screate_simple(1,dims,null)

def close(){
  H5.H5Sclose(id)
  }
}