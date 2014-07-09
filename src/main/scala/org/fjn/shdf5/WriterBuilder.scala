package org.fjn.shdf5

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag}
import  scala.reflect.runtime.{universe => ru}

/**
 * Created by fran on 09.07.2014.
 */
trait WriterBuilder {

  private var attributeFunctions = (x:Int)=>Unit::Nil

  private var location:Option[String] = None



}
