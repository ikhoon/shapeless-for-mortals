// Copyright (C) 2015 Sam Halliday
// License: http://www.apache.org/licenses/LICENSE-2.0
/**
 * TypeClass (api/impl/syntax) for marshalling objects into
 * `java.util.HashMap<String,Object>` (yay, big data!).
 */
package s4m.smbd

import shapeless._, labelled.{ field, FieldType }

/**
 * This exercise involves writing tests, only a skeleton is provided.
 *
 * - Exercise 1.1: derive =BigDataFormat= for sealed traits.
 * - Exercise 1.2: define identity constraints using singleton types.
 */
package object api {
  type StringyMap = java.util.HashMap[String, AnyRef]
  type BigResult[T] = Either[String, T] // aggregating errors doesn't add much
}

package api {
  trait BigDataFormat[T] {
    def label: String
    def toProperties(t: T): StringyMap
    def fromProperties(m: StringyMap): BigResult[T]
  }

  trait SPrimitive[V] {
    def toValue(v: V): AnyRef
    def fromValue(v: AnyRef): V
  }

  // EXERCISE 1.2
  trait BigDataFormatId[T, V] {
    def key: String
    def value(t: T): V
  }
}

package object impl {
  import api._

  // EXERCISE 1.1 goes here
   implicit def hNilBigDataFormat: BigDataFormat[HNil] =
     new BigDataFormat[HNil] {
       def label: String = "HNil"
       def toProperties(t: HNil): StringyMap = new StringyMap
       def fromProperties(m: StringyMap): BigResult[HNil] = Right(HNil)
     }

   implicit def hListBigDataFormat[K <: Symbol, V, L <: HList]
    (implicit
      key: Witness.Aux[K],
      tv: Typeable[V],
      F: BigDataFormat[L]
    ): BigDataFormat[FieldType[K, V] :: L] =
     new BigDataFormat[FieldType[K, V] :: L] {
       def label = key.value.name + " :: " + F.label

       def toProperties(t: FieldType[K, V] :: L): StringyMap = {
         val map: StringyMap = F.toProperties(t.tail)
         map.put(label, t.head)
         map
       }

       def fromProperties(m: StringyMap): BigResult[FieldType[K, V] :: L] =  {
         val v = tv.cast(m.get(key.value.name))
         v match {
           case Some(x) =>
             F.fromProperties(m) match {
               case Right(l) => Right(field[K](x) :: l)
               case Left(err) => Left(err + s", ${key.value.name} does not exists.")
             }
           case _ => Left(s"${key.value.name} type ${tv.describe} does not matched.")
         }
       }
     }

   implicit def cNilBigDataFormat: BigDataFormat[CNil] =
     new BigDataFormat[CNil] {
       def label: String = "CNil"

       def toProperties(t: CNil): StringyMap = new StringyMap

       def fromProperties(m: StringyMap): BigResult[CNil] = Left("nothing")
     }

   implicit def coproductBigDataFormat[K <: Symbol, V, T <: Coproduct]
    (implicit
      key: Witness.Aux[K],
      tv: Typeable[V],
      F: BigDataFormat[T]
    ): BigDataFormat[FieldType[K, V] :+: T] =
     new BigDataFormat[FieldType[K, V] :+: T] {
       def label = key.value.name + " :+: " + F.label
       def toProperties(t: FieldType[K, V] :+: T): StringyMap = t match {
         case Inl(l) =>
           val map = new StringyMap
           map.put(key.value.name, l)
           map
         case Inr(r) => F.toProperties(r)
       }

       def fromProperties(m: StringyMap): BigResult[FieldType[K, V] :+: T] = {
         val v = tv.cast(m.get(key))
         v match {
           case Some(x) =>
             Right(Inl(field[K](x)))
           case _ => F.fromProperties(m) match {
             case Right(r) => Right(Inr(r))
             case Left(err) => Left(err)
           }
         }
       }
     }


  implicit def familyBigDataFormat[T, R]
    (implicit
      gen: Generic.Aux[T, R],
      F: BigDataFormat[R]
    ): BigDataFormat[gen.Repr] = F
}

package impl {
  import api._

  // EXERCISE 1.2 goes here
}

package object syntax {
  import api._

  implicit class RichBigResult[R](val e: BigResult[R]) extends AnyVal {
    def getOrThrowError: R = e match {
      case Left(error) => throw new IllegalArgumentException(error.mkString(","))
      case Right(r) => r
    }
  }

  /** Syntactic helper for serialisables. */
  implicit class RichBigDataFormat[T](val t: T) extends AnyVal {
    def label(implicit s: BigDataFormat[T]): String = s.label
    def toProperties(implicit s: BigDataFormat[T]): StringyMap = s.toProperties(t)
    def idKey[P](implicit lens: Lens[T, P]): String = ???
    def idValue[P](implicit lens: Lens[T, P]): P = lens.get(t)
  }

  implicit class RichProperties(val props: StringyMap) extends AnyVal {
    def as[T](implicit s: BigDataFormat[T]): T = s.fromProperties(props).getOrThrowError
  }
}
