package org.fjn.monad

import ncsa.hdf.hdf5lib.H5

/**
 * Created by fran on 04/07/2014.
 */
case class DataSpaceManager(dimX:Long,dimY:Long){

  val id = H5.H5Screate_simple(2, Array(dimX,dimY), Array(dimX,dimY))

  def close(){
    H5.H5Sclose(id)}
}
