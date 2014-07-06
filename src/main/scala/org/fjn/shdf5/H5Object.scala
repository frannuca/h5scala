package org.fjn.shdf5

import ncsa.hdf.hdf5lib.{H5, HDF5Constants}

/**
 * Created by fran on 04/07/2014.
 */

/**
 * Abstract H5 object container managing the create,open and close actions on HDF5 files
 */
trait H5Object {

  /** file path of the hdf5 */
  val filePath: String

  /** * internal file id. This field is mutable since the user can open and close the file multiple times */
  private var fileId: Option[Int] = None


  /**
   * Truncate file, if it already exists, erasing all data previously stored in the file or creates a new one
   * @return   a simple H5Id instance containing the internal h5 internal file description for the root node
   */
  def create = {
    if (fileId.isEmpty)
      fileId = Some(H5.H5Fcreate(filePath, HDF5Constants.H5F_ACC_TRUNC,
        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT))


    new H5Id {
      val fid = fileId.getOrElse(-1)
    }
  }

  /**
   * Allow read-only access to file.
   * @return   a simple H5Id instance containing the internal h5 internal file description for the root node
   */
  def open = {
    if (fileId.isEmpty)
      fileId = Some(H5.H5Fopen(filePath, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT))


    new H5Id {
      val fid = fileId.getOrElse(-1)
    }
  }

  /**
   * Closes the file and removes the internal file descriptor is have been set before.
   * It is allowed to reopen or re-create the file once it has been closed
   */
  def close {
    fileId.foreach(H5.H5Fclose(_))
    fileId = None
  }

}


/**
 * Companion object containing all implicits H5MapTransformation.
 */
object H5Object {

  /**
   * given a path returns the correspondent H5Object
   * @param path   h5 file location
   * @return   H5Object instance
   */
  def apply(path: String): H5Object = {
    new H5Object {
      val filePath = path

    }
  }


  /**
   * operation injection of in and from  methods for H5Object instances. The operators "in" and "from" will be used
   * for later write or read calls
   * @param x
   * @return H5MonadOps base class without location
   */
  implicit def H5Mondad2Typed(x: H5Object): H5DirectionOps = {

    new H5DirectionOps {
      override val obj: H5Id = x.open
      override val locW = None
    }
  }

}