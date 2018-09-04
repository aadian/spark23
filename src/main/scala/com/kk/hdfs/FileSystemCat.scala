package com.kk.hdfs

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils

/**
  * @Auther: leiying
  * @Date: 2018/9/3 11:28
  * @Description:
  */
object FileSystemCat {
  def main(args: Array[String]): Unit = {

    val uri: String = "hdfs://nameservice1/performance_test/text.txt"
    val conf = new Configuration()
    conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem")
    val fileSystem: FileSystem = FileSystem.get(URI.create(uri), conf)
    var in: FSDataInputStream = null
    in = fileSystem.open(new Path(uri))
    IOUtils.copyBytes(in, System.out, 4096, false)

  }

}
