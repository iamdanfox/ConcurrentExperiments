package queue

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

  type DataArray = Array[Option[T]]

  val arrays = new Array[DataArray](20)
  arrays(0) = new DataArray(1)
  var end = (0, 0) // holds the first blank space after the queue

  /**
   * Decrement end and take the value that occupied its place
   */
  def pop: Option[T] = {
    val (chunkIndex, cell) = end

    if (cell > 0) {
      // just decrement
      end = (chunkIndex, cell - 1)
      return arrays(end._1)(end._2)
    } else {
      // nothing left in this array
      if (chunkIndex > 0) {
        // drop back to prev array
        end = (chunkIndex - 1, arrays(chunkIndex - 1).length - 1)
        arrays(chunkIndex) = null
        return arrays(end._1)(end._2) // TODO: assert this is always Some(-)
      } else {
        // nothing left at all
        return None
      } 
    }
  }

  /**
   * Insert a value and increment end, expanding capacity if necessary
   */
  def push(x: T): Boolean = {
    val (chunkIndex, cell) = end
    arrays(chunkIndex)(cell) = Some(x)
    val currentArrayCapacity = arrays(chunkIndex).length
    
    if (cell + 1 == currentArrayCapacity) {
      // need to extend
      end = (chunkIndex + 1, 0)
      arrays(chunkIndex + 1) = new DataArray(2 * currentArrayCapacity)
    } else {
      // just increment
      end = (chunkIndex, cell + 1)
    } 
    
    return true
  }

}

object UnboundedStack {

  def main(args: Array[String]) = {
    val st = new UnboundedStack[String]()
    st.push("Hello")
    st.push("!")
    println(st.pop)
    println(st.pop)
  }
}