package org.fjn.shdf5

import ncsa.hdf.hdf5lib.H5

/**
 * Created by fran on 04/07/2014.
 */

/**
 * Helper class to define the space and type allocation of the data to be transferred
 * @param dimX   first dimension
 * @param dimY   second dimension
 */
case class DataSpaceManager(dimX:Long,dimY:Long){

  val id = H5.H5Screate_simple(2, Array(dimX,dimY), Array(dimX,dimY))

  def close(){
    H5.H5Sclose(id)}
}
