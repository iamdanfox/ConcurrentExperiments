package queue
import java.util.concurrent.atomic.{ AtomicReferenceArray, AtomicReference }

class BoundedQueue[T](size: Int) {

  /**
   * config = (head, length, i, oldVal, newVal)
   *
   * Invariants:
   *    I.   array(i) never holds the same reference twice (for all i)
   *    II.  config never holds the same reference twice
   *    III. if config is successfully updated, then array(i) equalled newVal
   *    	 at some point before the update
   */

  protected val array = new AtomicReferenceArray[V](size)
  protected val config = new AtomicReference[(Int, Int, Int, V, V)](
    (0, 0, 0, null, Null()))

  protected trait V
  protected case class Null() extends V
  protected case class Value(x: T) extends V

  @annotation.tailrec final def dequeue: T = {
    // linearization point for failed calls:
    val oldConfig @ (head, length, i, oldVal, newVal) = config.get
    if (length == 0) return null.asInstanceOf[T]

    if (array.get(i) == newVal || array.compareAndSet(i, oldVal, newVal)) {
      val returnVal = if (head == i) newVal else array.get(head)
      val newConfig = ((head + 1) % size, length - 1, head, returnVal, Null())

      // linearization point for successful calls:
      if (config.compareAndSet(oldConfig, newConfig))
        return returnVal.asInstanceOf[Value].x
    }
    dequeue
  }

  @annotation.tailrec final def enqueue(x: T): Boolean = {
    // linearization point for failed calls:
    val oldConfig @ (head, length, i, oldVal, newVal) = config.get
    if (length == size) return false

    if (array.get(i) == newVal || array.compareAndSet(i, oldVal, newVal)) {
      val end = (head + length) % size
      val expectedNull = if (end == i) newVal else array.get(end)
      val newConfig = (head, length + 1, end, expectedNull, Value(x))

      // linearization point for successful calls:
      if (config.compareAndSet(oldConfig, newConfig)) return true
    }
    enqueue(x)
  }
}
