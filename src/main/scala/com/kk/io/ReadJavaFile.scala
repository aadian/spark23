package com.kk.io

import java.io.{File, IOException, PrintWriter}

import org.apache.commons.lang3.StringUtils

import scala.io.Source

/**
  * @Auther: leiying
  * @Date: 2018/9/6 16:30
  * @Description:
  */
object ReadJavaFile {

  def main(args: Array[String]): Unit = {
    //val rootPathStr = "E:\\项目SVN\\BMUGateway\\kl\\"
    val rootPathStr = "E:\\项目SVN\\BigdataMgt\\branches\\BigdataMgt_Dev\\3.0 代码"



    renameToScala(rootPathStr)

    //changeFileNameToJava(rootPathStr)





    //    val oldScalaFile = new File("E:\\test\\abc\\ab.java.scala")
//    val newJavaFile = new File("E:\\test\\abc\\ab.java")
//    oldScalaFile.renameTo(newJavaFile)



  }

  def changeFileNameToJava(rootPathStr:String)={

    val rootPath = new File(rootPathStr)
    val fileIter = subDir(rootPath)

    while (fileIter.hasNext) {

      val scalaFile = fileIter.next()
      val scalaPath = scalaFile.getAbsolutePath

      if (scalaPath.endsWith(".scala")) {
        val scalaFileName = scalaFile.getAbsolutePath
        println("scalaFileName:" +  scalaFileName)
        val javaFileName = scalaFileName.replaceAll(".scala","")
        println("javaFileName:" + javaFileName)

        val javaFile = new File(javaFileName)
        scalaFile.renameTo(javaFile)

      }
    }



  }

  def renameToScala(rootPathStr:String)={

    val rootPath = new File(rootPathStr)
    val fileIter = subDir(rootPath)

    while (fileIter.hasNext) {

      val oldJavaFile = fileIter.next()
      val oldJavaPath = oldJavaFile.getAbsolutePath
      if (oldJavaPath.endsWith(".java")) {
        try {
          var newScalaFile = new File(oldJavaPath + ".scala")
          println("newScalaFile:" + newScalaFile.getName)
          val newScalaFileOut = new PrintWriter(newScalaFile)

          val oldJavaFileSource = Source.fromFile(oldJavaPath)

          for (line <- oldJavaFileSource.getLines()) {
            newScalaFileOut.write(line + "\n")
          }
          newScalaFileOut.flush()
          newScalaFileOut.close()

          oldJavaFileSource.close()
          oldJavaFile.deleteOnExit()


        } catch {
          case e: Exception => println(e)
        }

      }
    }

  }


  def subDir(dir: File): Iterator[File] = {
    val dirs = dir.listFiles().filter(_.isDirectory())
    val files = dir.listFiles().filter(_.isFile())
    files.toIterator ++ dirs.toIterator.flatMap(subDir _)
  }


}
