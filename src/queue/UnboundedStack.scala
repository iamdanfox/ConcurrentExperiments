package queue
import java.util.concurrent.atomic.{ AtomicReferenceArray, AtomicReference }
import UnboundedStack.{Index, Chunk, ChunkUpdate, CellUpdate, V}

class UnboundedStack[T: scala.reflect.ClassTag] {

  val state: AtomicReference[(Index, ChunkUpdate, CellUpdate)] =
    new AtomicReference(((0, 0), None, ((0, 0), null, Null())))

  val arrays = new AtomicReferenceArray[Chunk](32)
  arrays.set(0, new Chunk(1))

  @annotation.tailrec final def push(x: T): Boolean = {
    val oldState @ (currentPushCell, chunkUpdate, cellUpdate) = state.get()
    increment(currentPushCell) match {
      case None => return false
      case Some(incremented) =>
        if (chunkCAS(chunkUpdate) && cellCAS(cellUpdate)) {
          val oldVal = arrayGet(currentPushCell)
          val newState = (incremented, fitChunksTo(currentPushCell), (currentPushCell, oldVal, Datum(x)))
          if (this.state.compareAndSet(oldState, newState))
            return true
        }
    }
    push(x) // otherwise recurse
  }

  @annotation.tailrec final def pop: Option[T] = {
    val oldState @ (pushCell, chunkUpdate, cellUpdate) = state.get()
    decrement(pushCell) match {
      case None => return None
      case Some(currentPopCell) =>
        if (chunkCAS(chunkUpdate) && cellCAS(cellUpdate)) {
          val rtn = arrayGet(currentPopCell)
          val newState = (currentPopCell, fitChunksTo(currentPopCell), (currentPopCell, rtn, Null()))
          if (this.state.compareAndSet(oldState, newState))
            return Some(rtn.asInstanceOf[Datum].x)
        }
    }
    pop // otherwise recurse
  }

  def cellCAS(cellUpdate: CellUpdate): Boolean = {
    val (index, expect, replace) = cellUpdate
    val arr = arrays.get(index._1)
    return arr != null &&
      (arr.get(index._2) == replace ||
        arr.compareAndSet(index._2, expect, replace))
  }

  def chunkCAS(chunkUpdate: ChunkUpdate): Boolean =
    chunkUpdate match {
      case None => true
      case Some((chIndex, expect, replace)) =>
        return arrays.get(chIndex) == replace ||
          arrays.compareAndSet(chIndex, expect, replace)
    }

  def fitChunksTo(index: Index): ChunkUpdate = {
    if (arrays.get(index._1) == null) { // ie we're submitting a push to a chunk that doesn't exist yet
      val newChunk = new Chunk((scala.math.pow(2, index._1)).asInstanceOf[Int])
      Some((index._1, null, newChunk))
    } else { // current push exists... can we delete a later one?
      val afterCurrent = arrays.get(index._1 + 1)
      if (afterCurrent != null)
        Some(index._1 + 1, afterCurrent, null)
      else
        None
    }
  }

  /**
   *  returns the next Index we can write to after filling argument index
   */ 
  def increment(index: Index): Option[Index] =
    if (index._1 < 32)
      if (index._2 == scala.math.pow(2, index._1) - 1)
        Some((index._1 + 1, 0))
      else
        Some((index._1, index._2 + 1))
    else
      None // will overflow INT_MAX on the second param

  def decrement(index: Index): Option[Index] =
    if (index._2 > 0)
      Some((index._1, index._2 - 1))
    else if (index._1 > 0)
      Some((index._1 - 1, arrays.get(index._1 - 1).length - 1))
    else
      None // can't decrement below (0,0)

  def arrayGet(pair: Index): V = {
    val arr = arrays.get(pair._1)
    if (arr == null)
      return null
    else
      arr.get(pair._2)
  }

  case class Null() extends V
  case class Datum(x: T) extends V
}

object UnboundedStack {
  trait V {}
  type Chunk = AtomicReferenceArray[V]
  type Index = (Int, Int)
  type CellUpdate = (Index, V, V)
  type ChunkUpdate = Option[(Int, Chunk, Chunk)]
}