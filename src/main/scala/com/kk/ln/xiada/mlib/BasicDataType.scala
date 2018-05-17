package com.kk.ln.xiada.mlib

import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD


/**
  * Created by leiying on 2018/5/14.
  */
object BasicDataType {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local").setAppName("demo")
    val sc = new SparkContext(conf)


    /*
    一、本地向量（Local Vector）
     */

    val dv: Vector = Vectors.dense(2.0, 0.0, 8.0)
    println(dv)
    //[2.0,0.0,8.0]

    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(2.0, 8.0))
    println(sv1)
    //(3,[0,2],[2.0,8.0])

    val sv2: Vector = Vectors.sparse(3, Seq((0, 2.0), (2, 8.0)))
    println(sv2)
    //(3,[0,2],[2.0,8.0])


    /*
    二、标注点（Labeled Point）
     */
    val pos = LabeledPoint(1.0, Vectors.dense(2.0, 0.0, 8.0))
    println(pos)
    //(1.0,[2.0,0.0,8.0])

    val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(2.0, 8.0)))
    println(neg)
    //(0.0,(3,[0,2],[2.0,8.0]))

    val examples = MLUtils.loadLibSVMFile(sc, "E:\\spark\\spark23\\src\\spark-master\\data\\mllib\\sample_libsvm_data.txt")
    println(examples.collect().head)
    """
      |(0.0,(692,[127,128,129,130,131,154,155,156,157,158,159,181,182,183,184,185,186,187,188,189,207,208,209,210,211,212,213,214,215,216,217,235,236,237,238,239,240,241,242,243,244,245,262,263,264,265,266,267,268,269,270,271,272,273,289,290,291,292,293,294,295,296,297,300,301,302,316,317,318,319,320,321,328,329,330,343,344,345,346,347,348,349,356,357,358,371,372,373,374,384,385,386,399,400,401,412,413,414,426,427,428,429,440,441,442,454,455,456,457,466,467,468,469,470,482,483,484,493,494,495,496,497,510,511,512,520,521,522,523,538,539,540,547,548,549,550,566,567,568,569,570,571,572,573,574,575,576,577,578,594,595,596,597,598,599,600,601,602,603,604,622,623,624,625,626,627,628,629,630,651,652,653,654,655,656,657],[51.0,159.0,253.0,159.0,50.0,48.0,238.0,252.0,252.0,252.0,237.0,54.0,227.0,253.0,252.0,239.0,233.0,252.0,57.0,6.0,10.0,60.0,224.0,252.0,253.0,252.0,202.0,84.0,252.0,253.0,122.0,163.0,252.0,252.0,252.0,253.0,252.0,252.0,96.0,189.0,253.0,167.0,51.0,238.0,253.0,253.0,190.0,114.0,253.0,228.0,47.0,79.0,255.0,168.0,48.0,238.0,252.0,252.0,179.0,12.0,75.0,121.0,21.0,253.0,243.0,50.0,38.0,165.0,253.0,233.0,208.0,84.0,253.0,252.0,165.0,7.0,178.0,252.0,240.0,71.0,19.0,28.0,253.0,252.0,195.0,57.0,252.0,252.0,63.0,253.0,252.0,195.0,198.0,253.0,190.0,255.0,253.0,196.0,76.0,246.0,252.0,112.0,253.0,252.0,148.0,85.0,252.0,230.0,25.0,7.0,135.0,253.0,186.0,12.0,85.0,252.0,223.0,7.0,131.0,252.0,225.0,71.0,85.0,252.0,145.0,48.0,165.0,252.0,173.0,86.0,253.0,225.0,114.0,238.0,253.0,162.0,85.0,252.0,249.0,146.0,48.0,29.0,85.0,178.0,225.0,253.0,223.0,167.0,56.0,85.0,252.0,252.0,252.0,229.0,215.0,252.0,252.0,252.0,196.0,130.0,28.0,199.0,252.0,252.0,253.0,252.0,252.0,233.0,145.0,25.0,128.0,252.0,253.0,252.0,141.0,37.0]))
      |
    """.stripMargin


    /*
    三、本地矩阵（Local Matrix）
     */

    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    print(dm)
    """
      |1.0  2.0
      |3.0  4.0
      |5.0  6.0
    """.stripMargin

    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    print(sm)
    """
      |3 x 2 CSCMatrix
      |(0,0) 9.0
      |(2,1) 6.0
      |(1,1) 8.0
    """.stripMargin


    /*
    * 四、分布式矩阵（Distributed Matrix）
    * */


    /*
    （一）行矩阵（RowMatrix）
     */

    val dv1: Vector = Vectors.dense(1.0, 2.0, 3.0)
    val dv2: Vector = Vectors.dense(2.0, 3.0, 4.0)
    val rows: RDD[Vector] = sc.parallelize(Array(dv1, dv2))
    val mat: RowMatrix = new RowMatrix(rows)
    mat.rows.foreach(println)
    """
      |[1.0,2.0,3.0]
      |[2.0,3.0,4.0]
    """.stripMargin

    val summary = mat.computeColumnSummaryStatistics()
    println("summary.count:" + summary.count)
    println("summary.max:" + summary.max)
    println("summary.mean:" + summary.mean)
    println("summary.normL1:" + summary.normL1)
    """
      |summary.count:2
      |summary.max:[2.0,3.0,4.0]
      |summary.mean:[1.5,2.5,3.5]
      |summary.normL1:[3.0,5.0,7.0]
    """.stripMargin

    /*
    （二）索引行矩阵（IndexedRowMatrix）
     */

    val idxr1 = IndexedRow(1, dv1)
    val idxr2 = IndexedRow(2, dv2)
    val idxrows = sc.parallelize(Array(idxr1, idxr2))
    val idxmat: IndexedRowMatrix = new IndexedRowMatrix(idxrows)
    idxmat.rows.foreach(println)
    """
      |IndexedRow(1,[1.0,2.0,3.0])
      |IndexedRow(2,[2.0,3.0,4.0])
    """.stripMargin


    /*
    （三）坐标矩阵（Coordinate Matrix）
     */
    val ent1 = new MatrixEntry(0, 1, 0.5)
    val ent2 = new MatrixEntry(2, 2, 1.8)
    val entries: RDD[MatrixEntry] = sc.parallelize(Array(ent1, ent2))
    val coordMat: CoordinateMatrix = new CoordinateMatrix(entries)
    coordMat.entries.foreach(println)
    """
      |MatrixEntry(0,1,0.5)
      |MatrixEntry(2,2,1.8)
    """.stripMargin


    // 将coordMat进行转置
    val transMat: CoordinateMatrix = coordMat.transpose()
    transMat.entries.foreach(println)
    """
      |MatrixEntry(1,0,0.5)
      |MatrixEntry(2,2,1.8)
    """.stripMargin


    // 将坐标矩阵转换成一个索引行矩阵
    val indexedRowMatrix = transMat.toIndexedRowMatrix()
    indexedRowMatrix.rows.foreach(println)
    """
      |IndexedRow(1,(3,[0],[0.5]))
      |IndexedRow(2,(3,[2],[1.8]))
    """.stripMargin


    /*
    （四）分块矩阵（Block Matrix）
     */

    // 创建8个矩阵项，每一个矩阵项都是由索引和值构成的三元组

    val entb1 = new MatrixEntry(0, 0, 1)
    val entb2 = new MatrixEntry(1, 1, 1)
    val entb3 = new MatrixEntry(2, 0, -1)
    val entb4 = new MatrixEntry(2, 1, 2)
    val entb5 = new MatrixEntry(2, 2, 1)
    val entb6 = new MatrixEntry(3, 0, 1)
    val entb7 = new MatrixEntry(3, 1, 1)
    val entb8 = new MatrixEntry(3, 3, 1)

    // 创建RDD[MatrixEntry]
    val bentries: RDD[MatrixEntry] = sc.parallelize(Array(entb1, entb2, entb3, entb4, entb5, entb6, entb7, entb8))

    // 通过RDD[MatrixEntry]创建一个坐标矩阵
    val bcoordMat: CoordinateMatrix = new CoordinateMatrix(bentries)

    // 将坐标矩阵转换成2x2的分块矩阵并存储，尺寸通过参数传入
    val matA: BlockMatrix = bcoordMat.toBlockMatrix(2, 2).cache()

    // 可以用validate()方法判断是否分块成功
    matA.validate()

    println(matA.toLocalMatrix)
    """
      |1.0   0.0  0.0  0.0
      |0.0   1.0  0.0  0.0
      |-1.0  2.0  1.0  0.0
      |1.0   1.0  0.0  1.0
    """.stripMargin



    // 查看其分块情况
    println(matA.numColBlocks)
    """
      |2
    """.stripMargin
    println(matA.numRowBlocks)
    """
      |2
    """.stripMargin


    // 计算矩阵A和其转置矩阵的积矩阵
    val ata = matA.transpose.multiply(matA)
    println(ata.toLocalMatrix())
    """
      |3.0   -1.0  -1.0  1.0
      |-1.0  6.0   2.0   1.0
      |-1.0  2.0   1.0   0.0
      |1.0   1.0   0.0   1.0
    """.stripMargin



  }

}
