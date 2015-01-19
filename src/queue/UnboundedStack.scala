package queue
import java.util.concurrent.atomic.{ AtomicReferenceArray, AtomicReference }

class UnboundedStack[T: scala.reflect.ClassTag] {

  type Chunk = AtomicReferenceArray[T]
  type Index = (Int, Int)

  val arrays = new Array[Chunk](20)
  arrays(0) = new Chunk(1)

  val cellUpdate = ((0, 0), null.asInstanceOf[T], null.asInstanceOf[T])
  // state = (firstBlank, cellUpdate)
  val state = new AtomicReference(((0, 0), cellUpdate))

  /**
   * Try to apply cellUpdate, (expanding/contracting as necessary) then
   * create new state, try to put back
   */
  @annotation.tailrec final def push(x: T): Boolean = {
    val oldState @ (firstBlank, (index, expect, replace)) = state.get()

    extendToInclude(index)
    val updated = arrayCAS(index, expect, replace)

    if (updated) {
      extendToInclude(firstBlank)
      // want to write into firstblank and increment it
      val oldVal = arrayGet(firstBlank)
      val newState = (increment(firstBlank), (firstBlank, oldVal, x))
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
        val updated = arrayCAS(index, expect, replace)
        if (updated) {
          // we know we can decrement
          val rtn = arrayGet(lastCell)
          val newState = (lastCell, (lastCell, rtn, null.asInstanceOf[T]))
          if (this.state.compareAndSet(oldState, newState)) {
            return Some(rtn)
          }
        }
      }
    }
    pop // otherwise recurse
  }

  // precondition: array(index._1) exists!. postcondition, cell holds `replace` (ie its idempotent)
  def arrayCAS(index: Index, expect: T, replace: T): Boolean =
    arrays(index._1).get(index._2) == replace || arrays(index._1).compareAndSet(index._2, expect, replace)

  def extendToInclude(index: Index) = {
    if (arrays(index._1) == null)
      arrays(index._1) = new Chunk(arrays(index._1 - 1).length * 2)
  }

  def shrinkIfPossible(index: Index) = {
    if (arrays(index._1 + 1) != null)
      arrays(index._1 + 1) = null
  }

  // returns the next Index we can write to after filling argument index
  def increment(index: Index): Index =
    if (index._2 == arrays(index._1).length - 1)
      (index._1 + 1, 0)
    else
      (index._1, index._2 + 1)

  def decrement(index: Index): Option[Index] =
    if (index._2 > 0)
      Some((index._1, index._2 - 1))
    else if (index._1 > 0)
      Some((index._1 - 1, arrays(index._1 - 1).length - 1))
    else
      None

  def arrayGet(pair: Index): T =
    arrays(pair._1).get(pair._2)

}

object UnboundedStack {

  def main(args: Array[String]) = {
    val st = new UnboundedStack[String]()
    //    st.push("Hello")
    //    st.push("!")
    //    assert(st.pop == Some("!"))
    //    assert(st.pop == Some("Hello"))
    println(st.push("a"))
    println(st.push("b"))
    println(st.push("c"))
    println(st.pop)
    println(st.pop)
    println(st.pop)
    println(st.pop)
    //    st.push("1")
    //    assert(st.pop == Some("1"))
    println("Done.")
  }
}