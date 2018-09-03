package com.kk.thread

import java.util.concurrent.{Callable, FutureTask}

/**
  * @Auther: leiying
  * @Date: 2018/8/30 16:52
  * @Description:
  */
object TestThread {

  def main(args: Array[String]): Unit = {

    val callable: Callable[String] = new Callable[String]() {
      @throws[Exception]
      override def call: String = {
        Thread.sleep(3000)
        "Hello World"
      }
    }

    // 将异步获取
    val task = new FutureTask[String](callable)
    new Thread(task).start()

    print(task.get)

    // 等待结果线程1// 等待结果线程1

//    val thread1 = new Thread(() => {
//      def foo() = try
//        System.out.println(task.get)
//      catch {
//        case e: InterruptedException =>
//          e.printStackTrace()
//        case e: Exception =>
//          e.printStackTrace()
//      }
//
//      foo()
//    })
//
//    // 等待结果线程2// 等待结果线程2
//
//    val thread2 = new Thread(() => {
//      def foo() = try
//        System.out.println(task.get)
//      catch {
//        case e: InterruptedException =>
//          e.printStackTrace()
//        case e: Exception =>
//          e.printStackTrace
//      }
//
//      foo()
//    })
//
//    thread1.start()
//    thread2.start()


  }







}
