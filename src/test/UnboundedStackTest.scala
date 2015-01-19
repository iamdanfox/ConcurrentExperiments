package test
import queue.UnboundedStack
import ox.cads.testing.LinearizabilityTester

object UnboundedStackTest {
  type MyImpl = UnboundedStack[String]
  type ReferenceImpl = scala.collection.immutable.List[String]

  def referencePopOp(impl: ReferenceImpl): (Option[String], ReferenceImpl) =
    if (impl.isEmpty)
      (None, impl)
    else {
      (Some(impl.head), impl.tail)
    }

  def referencePushOp(x: String)(impl: ReferenceImpl): (Boolean, ReferenceImpl) =
    (true, impl.+:(x))

  def worker(me: Int, tester: LinearizabilityTester[ReferenceImpl, MyImpl]) = {
    val random = new scala.util.Random
    for (i <- 0 until 200) {
      val x = random.nextInt(200).toString + "x"
      if (random.nextFloat() < 0.6)
        tester.log(me, _.pop, "pop", referencePopOp)
      else
        tester.log(me, _.push(x), "push(" + x + ")", referencePushOp(x))
    }
  }

  def main(args: Array[String]) = {
    for (i <- 0 until 500) {
      println("loop iteration " + i)
      val myImpl = new MyImpl()
      val tester = new LinearizabilityTester(scala.collection.immutable.List[String](), myImpl, 4, worker, 800)
      if (tester() <= 0) {
        assert(false)
      }
    }
    println("Done")
  }
}
