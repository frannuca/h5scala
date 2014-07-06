package org.fjn.shdf5

/**
 * Created by fran on 05.07.2014.
 */
/**
 * Simple addition of a slash to the directory path inside the html5 file location
 */
object CorrectPath {

  implicit def pathCorrection(path:String)= new {
    def toCorrectedH5Path: String = if(path.last == '/') path else path + "/"
  }
}
