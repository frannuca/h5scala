package org.fjn.monad

/**
 * Created by fran on 05.07.2014.
 */
object CorrectPath {

  implicit def pathCorrection(path:String)= new {
    def toCorrectedH5Path: String = if(path.last == '/') path else path + "/"
  }
}
