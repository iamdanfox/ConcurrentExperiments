package queue
import java.util.concurrent.atomic.{ AtomicReferenceArray, AtomicReference }

class UnboundedStack[T: scala.reflect.ClassTag] {

  trait V {}
  case class Null() extends V
  case class Datum(x: T) extends V

  type Chunk = AtomicReferenceArray[V]
  type Index = (Int, Int)

  val arrays = new AtomicReferenceArray[Chunk](10)
  arrays.set(0, new Chunk(1))

  val cellUpdate = ((0, 0), null.asInstanceOf[V], Null().asInstanceOf[V])
  // state = (firstBlank, cellUpdate)
  val state = new AtomicReference(((0, 0), cellUpdate))

  /**
   * Try to apply cellUpdate, (expanding/contracting as necessary) then
   * create new state, try to put back
   */
  @annotation.tailrec final def push(x: T): Boolean = {
    val oldState @ (firstBlank, (index, expect, replace)) = state.get()

    extendToInclude(index)
    if (arrayCAS(index, expect, replace)) {
      // want to write into firstblank and increment it
      val oldVal = arrayGet(firstBlank)
      val newState = (increment(firstBlank), (firstBlank, oldVal, Datum(x)))
      if (this.state.compareAndSet(oldState, newState)) {
        return true
      }
    }
    push(x) // otherwise recurse
  }

  @annotation.tailrec final def pop: Option[T] = {
    val oldState @ (firstBlank, (index, expect, replace)) = state.get()

    decrement(firstBlank) match {
      case None => return None
      case Some(lastCell) => {
        extendToInclude(index)
        if (arrayCAS(index, expect, replace)) {
          // we know we can decrement
          val rtn = arrayGet(lastCell)
          val newState = (lastCell, (lastCell, rtn, Null()))
          if (this.state.compareAndSet(oldState, newState)) {
            return Some(rtn.asInstanceOf[Datum].x)
          }
        }
      }
    }
    pop // otherwise recurse
  }

  // precondition: array(index._1) exists!. postcondition, cell holds `replace` (ie its idempotent)
  def arrayCAS(index: Index, expect: V, replace: V): Boolean =
    arrays.get(index._1).get(index._2) == replace || arrays.get(index._1).compareAndSet(index._2, expect, replace)

  def extendToInclude(index: Index) = {
    if (arrays.get(index._1) == null) {
      val newArray = new Chunk(arrays.get(index._1 - 1).length * 2);
      arrays.compareAndSet(index._1, null, newArray)
    }
  }

  // returns the next Index we can write to after filling argument index
  def increment(index: Index): Index =
    if (index._2 == scala.math.pow(2,index._1) - 1)
      (index._1 + 1, 0)
    else
      (index._1, index._2 + 1)

  def decrement(index: Index): Option[Index] =
    if (index._2 > 0)
      Some((index._1, index._2 - 1))
    else if (index._1 > 0)
      Some((index._1 - 1, arrays.get(index._1 - 1).length - 1))
    else
      None

  def arrayGet(pair: Index): V = {
    val arr = arrays.get(pair._1)
    if (arr == null)
      return null
    else
      arr.get(pair._2)
  }
}

object UnboundedStack {

  def main(args: Array[String]) = {
    val st = new UnboundedStack[String]()
    println(st.push("a"))
    println(st.push("b"))
    println(st.push("c"))
    println(st.pop)
    println(st.pop)
    println(st.pop)
    println(st.pop)
    println("Done.")
  }
}