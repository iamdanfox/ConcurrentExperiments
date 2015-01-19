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
    val oldState @ (nextPushCell, chCAS, (index, expect, replace)) = state.get()
    if (chunkCAS(chCAS) && cellCAS(index, expect, replace)) {
      val oldVal = arrayGet(nextPushCell)
      val newState = (increment(nextPushCell), fitTo(nextPushCell), (nextPushCell, oldVal, Datum(x)))
      if (this.state.compareAndSet(oldState, newState))
        return true
    }
    push(x) // otherwise recurse
  }

  @annotation.tailrec final def pop: Option[T] = {
    val oldState @ (nextPushCell, chCAS, (index, expect, replace)) = state.get()
    decrement(nextPushCell) match {
      case None => return None
      case Some(decremented) => {
        if (chunkCAS(chCAS) && cellCAS(index, expect, replace)) {
          val rtn = arrayGet(decremented)
          val newState = (decremented, fitTo(decremented), (decremented, rtn, Null()))
          if (this.state.compareAndSet(oldState, newState)) 
            return Some(rtn.asInstanceOf[Datum].x)
        }
      }
    }
    pop // otherwise recurse
  }

  def cellCAS(index: Index, expect: V, replace: V): Boolean = {
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

  def fitTo(index: Index): Option[(Int, Chunk, Chunk)] = {
    if (arrays.get(index._1) == null) { // ie we're submitting a push to a chunk that doesn't exist yet
      val newChunk = new Chunk(scala.math.pow(2, index._1).asInstanceOf[Int])
      Some((index._1, null, newChunk))
    }else {
      val afterChunk = arrays.get(index._1 + 1)
      if (afterChunk != null) // no need to keep a whole empty one
        Some(index._1 + 1, afterChunk, null)
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