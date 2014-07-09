package org.fjn.shdf5

import ncsa.hdf.hdf5lib.{HDF5Constants, H5}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag}
import  scala.reflect.runtime.{universe => ru}

/**
 * Created by fran on 05.07.2014.
 */

/**
 * Monadic Operation trait for writing and reading operation in hdf5 files
 * This trait is used as based for writing and reading derived trait. Two methods are injected from this trade depencing on the
 * operation being instanciated: read, write
 */
trait H5Ops{

  self:H5Ops =>


  import org.fjn.shdf5.CorrectPath._


  val obj:H5Id
  protected val locW:Option[String]


  /**
   * when applied set the location of the folder to be written and returns a writing monad object
   * @param location  path to the place where we want to write
   * @return H5MonadOpsWriter where to apply the method write
   */
  def ingroup(location:String)=new H5OpsWriter{
    val locW = Some(location.toCorrectedH5Path)
    val obj = self.obj

  }


  /**
   * places the internal path reference to the place where to read data from
   * @param location  full path till the folder containing the dataset to be read
   * @return    H5OpsReader where to apply the method read
   */
  def fromgroup(location:String)=new H5OpsReader{
    val locW = Some(location.toCorrectedH5Path)
    val obj = self.obj
  }

}



