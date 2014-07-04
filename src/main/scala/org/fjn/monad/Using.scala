package org.fjn.monad

/**
 * Created by fran on 04/07/2014.
 */
object Using{
  def using[A<:{def close():Unit},B](v:A)(f: A=>B){
    try{
      f(v)
    }
    finally{
      v.close
    }
  }
}
