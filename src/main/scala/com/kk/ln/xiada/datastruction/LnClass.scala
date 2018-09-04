package com.kk.ln.xiada.datastruction

/**
  * Created by leiying on 2018/5/17.
  */

class Counter {
  private var value = 0

  def increment(): Unit = {
    value + 1
  }

  def current(): Int = {
    value
  }
}


object LnClass {

  def main(args: Array[String]): Unit = {
    val ct = new Counter()
    ct.increment
    println(ct.current)

  }

}

/**
  * Scala中的继承与Java有着显著的不同：
  * （1）重写一个非抽象方法必须使用override修饰符。
  * （2）只有主构造器可以调用超类的主构造器。
  * （3）在子类中重写超类的抽象方法时，不需要使用override关键字。
  * （4）可以重写超类中的字段。
  */

abstract class Car {
  //是抽象类，不能直接被实例化
  val carBrand: String

  //字段没有初始化值，就是一个抽象字段
  def info()

  //抽象方法，不需要使用abstract关键字
  def greeting() {
    println("Welcome to my car!")
  }
}

class BMWCar extends Car {
  override val carBrand = "BMW"

  //重写超类字段，需要使用override关键字，否则编译会报错
  def info() {
    printf("This is a %s car. It is on sale", carBrand)
  }

  //重写超类的抽象方法时，不需要使用override关键字，不过，如果加上override编译也不错报错
  override def greeting() {
    println("Welcome to my BMW car!")
  } //重写超类的非抽象方法，必须使用override关键字
}

class BYDCar extends Car {
  override val carBrand = "BYD"

  //重写超类字段，需要使用override关键字，否则编译会报错
  def info() {
    printf("This is a %s car. It is cheap.", carBrand)
  }

  //重写超类的抽象方法时，不需要使用override关键字，不过，如果加上override编译也不错报错
  override def greeting() {
    println("Welcome to my BYD car!")
  } //重写超类的非抽象方法，必须使用override关键字
}

object MyCar {
  def main(args: Array[String]) {
    val myCar1 = new BMWCar()
    val myCar2 = new BYDCar()
    myCar1.greeting()
    myCar1.info()
    myCar2.greeting()
    myCar2.info()
  }
}

/**
  * “特质(trait)”，它不仅实现了接口的功能，还具备了很多其他的特性。
  * Scala的特质，是代码重用的基本单元，可以同时拥有抽象方法和具体方法。
  * Scala中，一个类只能继承自一个超类，却可以实现多个特质，
  * 从而重用特质中的方法和字段，实现了多重继承。
  */

trait CarId {
  var id: Int

  def currentId(): Int //定义了一个抽象方法
}

trait CarGreeting {
  def greeting(msg: String) {
    println(msg)
  }
}

class BYDCarId extends CarId with CarGreeting {
  //使用extends关键字混入第1个特质，后面可以反复使用with关键字混入更多特质
  override var id = 10000

  //BYD汽车编号从10000开始
  def currentId(): Int = {
    id += 1;
    id
  } //返回汽车编号
}

class BMWCarId extends CarId with CarGreeting {
  //使用extends关键字混入第1个特质，后面可以反复使用with关键字混入更多特质
  override var id = 20000

  //BMW汽车编号从10000开始
  def currentId(): Int = {
    id += 1;
    id
  } //返回汽车编号
}

object MyCarId {
  def main(args: Array[String]) {
    val myCarId1 = new BYDCarId()
    val myCarId2 = new BMWCarId()
    myCarId1.greeting("Welcome my first car.")
    printf("My first CarId is %d.\n", myCarId1.currentId)
    myCarId2.greeting("Welcome my second car.")
    printf("My second CarId is %d.\n", myCarId2.currentId)
  }

  """
    |Welcome my first car.
    |My first CarId is 10001.
    |Welcome my second car.
    |My second CarId is 20001.
  """.stripMargin
}


/**
  * Scala中的模式匹配的功能则要强大得多，可以应用到switch语句、类型检查、“解构”等多种场合。
  */

object PatternsMatching {
  def main(args: Array[String]): Unit = {
    for (elem <- List(9, 12.3, "Spark", "Hadoop", 'Hello)) {
      val str = elem match {
        case i: Int => i + " is an int value."
        case d: Double => d + " is a double value."
        case "Spark" => "Spark is found."
        case s: String => s + " is a string value."
        case _ => "This is an unexpected value."
      }
      println(str)
    }

    """
      |9 is an int value.
      |12.3 is a double value.
      |Spark is found.
      |Hadoop is a string value.
      |This is an unexpected value.
    """.stripMargin

    //“模式中增加守卫(guard)”语句
    for (elem <- List(1, 2, 3, 4)) {
      elem match {
        case _ if (elem % 2 == 0) => println(elem + " is even.")
        case _ => println(elem + " is odd.")
      }
    }
    """
      |1 is odd.
      |2 is even.
      |3 is odd.
      |4 is even.
    """.stripMargin

    //for表达式中的模式
    val university = Map("XMU" -> "Xiamen University", "THU" -> "Tsinghua University", "PKU" -> "Peking University")
    for ((k, v) <- university) printf("Code is : %s and name is: %s\n", k, v)
    """
      |Code is : XMU and name is: Xiamen University
      |Code is : THU and name is: Tsinghua University
      |Code is : PKU and name is: Peking University
    """.stripMargin


    //case类的匹配
    case class Car(brand: String, price: Int)
    val myBYDCar = new Car("BYD", 89000)
    val myBMWCar = new Car("BMW", 1200000)
    val myBenzCar = new Car("Benz", 1500000)
    for (car <- List(myBYDCar, myBMWCar, myBenzCar)) {
      car match {
        case Car("BYD", 89000) => println("Hello, BYD!")
        case Car("BMW", 1200000) => println("Hello, BMW!")
        case Car(brand, price) => println("Brand:" + brand + ", Price:" + price + ", do you want it?")
      }
    }
    """
      |Hello, BYD!
      |Hello, BMW!
      |Brand:Benz, Price:1500000, do you want it?
    """.stripMargin

    //Option类型
    /**
      * Option类包含一个子类Some，当存在可以被引用的值的时候，就可以使用Some来包含这个值，
      * 例如Some(“Hadoop”)。而None则被声明为一个对象，而不是一个类，表示没有值。
      */
    val books = Map("hadoop" -> 5, "spark" -> 10, "hbase" -> 7)
    val hadoopBook = books.get("hadoop")
    println(hadoopBook)
    """
      |Some(5)
    """.stripMargin
    val sparkBook = books.get("hive")
    //println(sparkBook.getClass.getTypeName)
    """
      |scala.None$
    """.stripMargin
    println(books.get("hive").getOrElse("no such a book"))
    """
      |no such a book
    """.stripMargin

  }


}

object HighOrderFunc {
  def main(args: Array[String]): Unit = {

    def sum(f: Int => Int, a: Int, b: Int): Int = {
      if (a > b) 0 else f(a) + sum(f, a + 1, b)
    }

    def self(x: Int): Int = x

    def square(x: Int): Int = x * x

    def powerOfTwo(x: Int): Int = if (x == 0) 1 else 2 * powerOfTwo(x - 1)

    def sumInts(a: Int, b: Int): Int = sum(self, a, b)

    def sumSquared(a: Int, b: Int): Int = sum(square, a, b)

    def sumPowersOfTwo(a: Int, b: Int): Int = sum(powerOfTwo, a, b)

    println(sumInts(1, 5))
    println(sumSquared(1, 5))
    println(sumPowersOfTwo(1, 5))

    """
      |15
      |55
      |62
    """.stripMargin

  }
}

