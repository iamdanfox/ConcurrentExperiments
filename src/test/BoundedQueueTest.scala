package test
import queue.BoundedQueue
import ox.cads.testing.LinearizabilityTester

object BoundedQueueTest {
  type MyImpl = BoundedQueue[String]
  type ReferenceImpl = scala.collection.immutable.Queue[String]

  val CAPACITY = 10

  def referenceDequeueOp(impl: ReferenceImpl): (String, ReferenceImpl) =
    if (impl.isEmpty) (null, impl) else impl.dequeue

  def referenceEnqueueOp(x: String)(impl: ReferenceImpl): (Boolean, ReferenceImpl) =
    if (impl.length < CAPACITY) (true, impl.enqueue(x)) else (false, impl)

  def worker(me: Int, tester: LinearizabilityTester[ReferenceImpl, MyImpl]) = {
    val random = new scala.util.Random
    for (i <- 0 until 200) {
      val x = random.nextInt(200).toString + "x"
      if (random.nextFloat() < 0.5)
        tester.log(me, _.dequeue, "dequeue", referenceDequeueOp)
      else
        tester.log(me, _.enqueue(x), "enqueue(" + x + ")", referenceEnqueueOp(x))
    }
  }

  def main(args: Array[String]) = {
    for (i <- 0 until 500) {
      println("loop iteration " + i)
      val myImpl = new MyImpl(CAPACITY)
      val tester = new LinearizabilityTester(
        scala.collection.immutable.Queue[String](), myImpl, 4, worker, 800)
      if (tester() <= 0) {
        assert(false)
      }
    }
    println("Done")
  }
}
