package com.kk.thread

import java.util.concurrent.{Callable, FutureTask, Executors, ExecutorService}

/**
  * @Auther: leiying
  * @Date: 2018/8/30 17:11
  * @Description:
  */
object TestCallable {

  def main(args: Array[String]) {
    val threadPool:ExecutorService=Executors.newFixedThreadPool(3)
    try {
      val future=new FutureTask[String](new Callable[String] {
        override def call(): String = {
          Thread.sleep(100)
          return "im result"
        }
      })
      threadPool.execute(future)
      println(future.get())
      println(future.get())
      println(future.get())
    }finally {
      threadPool.shutdown()
    }
  }

}
