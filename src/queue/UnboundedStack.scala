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
  val chCAS = None.asInstanceOf[Option[(Int, Chunk, Chunk)]]
  val nextPush = (0, 0)
  val state = new AtomicReference((nextPush, chCAS, cellUpdate))

  /**
   * Try to apply cellUpdate, (expanding/contracting as necessary) then
   * create new state, try to put back
   */
  @annotation.tailrec final def push(x: T): Boolean = {
    val oldState @ (nextPush, chCAS, (index, expect, replace)) = state.get()

    if (chunkCAS(chCAS) && arrayCAS(index, expect, replace)) {
      // want to write into firstblank and increment it
      val oldVal = arrayGet(nextPush)

      val newState = (increment(nextPush), extendToInclude(nextPush), (nextPush, oldVal, Datum(x)))
      if (this.state.compareAndSet(oldState, newState)) {
        return true
      }
    }
    push(x) // otherwise recurse
  }

  @annotation.tailrec final def pop: Option[T] = {
    val oldState @ (nextPush, chCAS, (index, expect, replace)) = state.get()

    decrement(nextPush) match {
      case None => return None
      case Some(lastCell) => {
        if (chunkCAS(chCAS) && arrayCAS(index, expect, replace)) {
          // we know we can decrement
          val rtn = arrayGet(lastCell)

          val newState = (lastCell, extendToInclude(lastCell), (lastCell, rtn, Null()))
          if (this.state.compareAndSet(oldState, newState)) {
            return Some(rtn.asInstanceOf[Datum].x)
          }
        }
      }
    }
    pop // otherwise recurse
  }

  def arrayCAS(index: Index, expect: V, replace: V): Boolean = {
    val arr = arrays.get(index._1)
    return arr != null &&
      (arr.get(index._2) == replace ||
        arr.compareAndSet(index._2, expect, replace))
  }

  def chunkCAS(chCAS: Option[(Int, Chunk, Chunk)]): Boolean =
    chCAS match {
      case None => true
      case Some((chIndex, expect, replace)) =>
        arrays.get(chIndex) == replace || arrays.compareAndSet(chIndex, expect, replace)
    }

  def extendToInclude(index: Index): Option[(Int, Chunk, Chunk)] = {
    if (arrays.get(index._1) == null) // ie we're submitting a push to a chunk that doesn't exist yet
      Some((index._1, null, new Chunk(scala.math.pow(2, index._1).asInstanceOf[Int])))
    else {
      val after = arrays.get(index._1 + 1)
      if (after != null) // no need to keep a whole empty one
        Some(index._1 + 1, after, null)
      else
        None
    }
  }

  // returns the next Index we can write to after filling argument index
  def increment(index: Index): Index =
    if (index._2 == scala.math.pow(2, index._1) - 1)
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