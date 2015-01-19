package queue
import java.util.concurrent.atomic.{ AtomicReferenceArray, AtomicReference }

/**
 * Idea: head and tail pointers into an array work well, but what happens when we reach the end?
 * Create another array, and let the tail pointer continue into that array!
 * pointers would then be pairs (int, int) that represent which array to look in and which cell within that array.
 * Maintain an array of say, 20 slots, each of which can contain an array.
 * Initially, only the first slot holds an array of maybe 16 slots.  When this is
 * filled, we create a new array of 32 slots and store a reference to it in the top
 * level array's second cell.  When the head pointer leaves one of the child arrays, we can destroy it completely.
 * ADVANTAGE: it doesn't require copying to resize, so good for concurrency (we avoid lots of threads trying to copy things and one failing)
 * benchmark against normal one (maybe protected by a 'farmer' proc, handing stuff to workers)
 *
 */
class UnboundedStack[T: scala.reflect.ClassTag] {

  type Chunk = Array[T]
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
  def push(x: T): Boolean = {
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

  // precondition: array(index._1) exists!. postcondition, cell holds `replace` (ie its idempotent)
  def arrayCAS(index: Index, expect: T, replace: T): Boolean = {
    arrays(index._1)(index._2) = replace // TODO CAS
    return true
  }

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

  def arrayGet(pair: Index): T =
    arrays(pair._1)(pair._2)

}

object UnboundedStack {

  def main(args: Array[String]) = {
    val st = new UnboundedStack[String]()
    //    st.push("Hello")
    //    st.push("!")
    //    assert(st.pop == Some("!"))
    //    assert(st.pop == Some("Hello"))
    println(st.push("a"))
    println(st.push("a"))
    println(st.push("a"))
    //    st.push("1")
    //    assert(st.pop == Some("1"))
    println("Done.")
  }
}