package org.fjn.monad

/**
 * Created by fran on 06.07.2014.
 */
object runable extends App{

//  new TestWritingReading().readWriteTypes
//
//  1


  sealed trait MayBe[+A] {
    def flatMap[B](f:A=>MayBe[B]):MayBe[B]

    def map[B](f:A=>B):MayBe[B] = flatMap(a => Yes(f(a)))

    def flatten[B](implicit asMaybeMaybe: MayBe[A] <:< MayBe[MayBe[B]]): MayBe[B] =
      asMaybeMaybe(this) flatMap identity
  }

  case class Yes[+A](a:A) extends MayBe[A]{
    def flatMap[B](f:A=>MayBe[B]):MayBe[B]= f(a)
  }

  case object No extends MayBe[Nothing]{
    def flatMap[B](f: Nothing => MayBe[B]):MayBe[B]=No
  }


  trait Functor[M[_]] {
    def map[A,B](f:A=>B):M[A]=>M[B]
  }

  implicit object MayBeFunctor extends Functor[MayBe] {
    def map[A,B](f:A=>B):MayBe[A]=>MayBe[B]= maybe => maybe map f
  }

  val rain = Yes(67)

  for(i <- rain) yield{
    1
  }
}
