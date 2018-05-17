package com.kk.ln.xiada.datastruction

/**
  * Created by leiying on 2018/5/17.
  */

object Student {
  private var lastId = 0

  //一个人的身份编号
  def newStudentId() = {
    lastId += 1
    lastId
  }
}


object LnObject {

  def main(args: Array[String]): Unit = {
    printf("The first person id is %d.\n", Student.newStudentId())
    printf("The second person id is %d.\n", Student.newStudentId())
    printf("The third person id is %d.\n", Student.newStudentId())
  }
  """
    |The first person id is 1.
    |The second person id is 2.
    |The third person id is 3.
  """.stripMargin


}


/**
  * 伴生类Person中的成员和伴生对象Person中的成员都被合并到一起，并且，伴生对象中的方法newPersonId()，成为静态方法。
  */
class Person {
  private val id = Person.newPersonId()
  //调用了伴生对象中的方法
  private var name = ""

  def this(name: String) {
    this()
    this.name = name
  }

  def info() {
    printf("The id of %s is %d.\n", name, id)
  }
}

object Person {
  private var lastId = 0

  //一个人的身份编号
  private def newPersonId() = {
    lastId += 1
    lastId
  }

  def main(args: Array[String]) {
    val person1 = new Person("Ziyu")
    val person2 = new Person("Minxing")
    person1.info()
    person2.info()
  }
  """
    |The id of Ziyu is 1.
    |The id of Minxing is 2.
  """.stripMargin
}