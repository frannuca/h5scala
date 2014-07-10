package org.fjn.shdf5

import org.junit.Assert._

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Created by fran on 10.07.2014.
 */
import scala.reflect.runtime.{universe => ru}

object RandomMatrixGenerators {

  val rngGen = new java.util.Random()


  def createMatrixOf[A:ClassTag:ru.TypeTag](nrows:Int,ncols:Int): Array[Array[A]] ={


     (for(i <- 0 until nrows)  yield {
      Array.ofDim[A](ncols).transform(_ =>{
        ru.typeOf[A] match{
          case t  if t =:= ru.typeOf[Int] =>   rngGen.nextInt().asInstanceOf[A]
          case t  if t =:= ru.typeOf[Long] =>   rngGen.nextLong().asInstanceOf[A]
          case t  if t =:= ru.typeOf[Double] =>   rngGen.nextDouble().asInstanceOf[A]
          case t if t =:= ru.typeOf[Float] =>   rngGen.nextFloat().asInstanceOf[A]
          case t if t =:= ru.typeOf[Byte] =>  {val aux= Array.ofDim[Byte](1); rngGen.nextBytes(aux);aux.head.asInstanceOf[A]}
          case t => throw new Throwable("Unsupported test type")

        }

      }).toArray
    }).toArray

  }

}
