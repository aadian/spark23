package com.kk.hbase

import java.io.{File, FileWriter}
import java.net.URI
import java.util.{ArrayList, Arrays}

import com.kk.util.HDFSHelper
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.mutable.ListBuffer

/**
  * 获取表的信息，并将对应信息写入日志文件
  *
  * @param outputBasePath 日志文件的目录
  * @param testTableName  表名
  */
class ThreadGetTableInfo(uri:String, tableNameDir: String, outputBasePath: String, testTableName: String) extends Thread {

  //使用map存储日志文件，新创建的文件存储在此数据结构中
  //var fileMap: Map[String, FileWriter] = Map()
  //var fileMap: Map[String, String] = Map()
  var fileMap = new java.util.HashMap[String, FileWriter]()

  val conf = HBaseConfiguration.create()


  //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
  conf.set("hbase.zookeeper.quorum", "localhost")
  //设置zookeeper连接端口，默认2181
  conf.set("hbase.zookeeper.property.clientPort", "2181")

  val connection = ConnectionFactory.createConnection(conf);
  val admin = connection.getAdmin();

  val hdfsConf = new Configuration()
  hdfsConf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem")

  val fs = FileSystem.get(URI.create(uri), hdfsConf)


  override def run(): Unit = {

    while (true) {
      val clusterStatus = admin.getClusterStatus
      val serverIterator = clusterStatus.getServers.iterator()
      while (serverIterator.hasNext) {
        //服务名称
        val serverName = serverIterator.next()
        val serverLoad = clusterStatus.getLoad(serverName)
        val regLoadIt = serverLoad.getRegionsLoad.entrySet().iterator()

        while (regLoadIt.hasNext) {
          val regLoad = regLoadIt.next()

          /** 获取表名
            * regLoad内容，注意表名带了schema
            * regionLoadStr:PHERF.USER_DEFINED_TEST,,1535955578393.167704143b4abad814b3ec6d82df505e.
           */
          val regionLoadStr = Bytes.toString(regLoad.getKey)
          val tableName = regionLoadStr.split(",")(0) //TEST_PERFORMANCE_T1


          //如果是需要监控的数据表
          if (tableName.equalsIgnoreCase(testTableName)) {
            //获取region名称
            val regionName = regionLoadStr.split("\\.")(2) //f9b17a4f0380f22ab9d80d77b1e62e50

            println("RegionName:" + regionName + "," + System.currentTimeMillis() + ",MemStoreSize:" + regLoad.getValue.getMemStoreSizeMB())

            //当前region的Memstore大小
            logMemSize(regionName, regLoad.getValue.getMemStoreSizeMB)

            //获取此表的列族
            val table = connection.getTable(TableName.valueOf(tableName))
            val familyNames = new ArrayList[String]
            import scala.collection.JavaConversions._
            for (family <- table.getTableDescriptor.getFamilies) {
              familyNames.add(family.getNameAsString)
            }

            //通过region名称和列族名称，获取region等相关信息
            getRegionFileInfo(uri, tableNameDir, regionName, familyNames)
          }
        }


      }

      Thread.sleep(1000)
    }


  }

  /**
    * 记录MemStore的大小
    *
    * @param regionName
    * @param memStoreSize
    */
  def logMemSize(regionName: String, memStoreSize: Int): Unit = {
    val stringArr = Arrays.asList("MemStoreSize", regionName)
    val actualFileName = StringUtils.join(stringArr.toArray, '_')
    println("actualFileName:" + actualFileName)
    if (fileMap.containsKey(actualFileName)) {
      val out = fileMap.get(actualFileName)
      out.write("[" + System.currentTimeMillis() + "," + memStoreSize + "],\n")
      out.flush()
    } else {
      val out = new FileWriter(outputBasePath + actualFileName , true)
      //val fileName = actualFileName
      fileMap.put(actualFileName, out)
      out.write("[" + System.currentTimeMillis() + "," + memStoreSize + "],\n")
      out.flush()
    }
  }


  /**
    * 记录单个列族下单个文件的大小
    *
    * @param regionName region名称
    * @param cfName     列族名称
    * @param fileName   列族对应的文件名称
    * @param fileSize   文件大小
    */
  def logFileSize(regionName: String, cfName: String, fileName: String, fileSize: Long): Unit = {

    //构造字符串数组，并用下划线连接起来，从而构造日志文件名称
    val stringArr = Arrays.asList("FileSize", regionName, cfName, fileName)
    val actualFileName = StringUtils.join(stringArr.toArray, '_')

    //如果文件已经存在，直接写入文件内容
    if (fileMap.containsKey(actualFileName)) {
      val out = fileMap.get(actualFileName)
      out.write("[" + System.currentTimeMillis() + "," + fileSize / (1024 * 1024) + "],\n")
      out.flush()
    } else {
      //如果文件不存在，先创建文件再写入文件内容

      val out = new FileWriter(outputBasePath + actualFileName , true)
      val fileName = actualFileName
      fileMap.put(actualFileName, out)
      out.write("[" + System.currentTimeMillis() + "," + fileSize / (1024 * 1024) + "],\n")
      out.flush()

    }

    //memStore_RegionName_Size	filesize_fileName_RegionName_CF_Size	fileNum_fileName_Region_CF_Number
  }

  /**
    * 记录region单个列族的文件数量
    *
    * @param regionName region名称
    * @param cfName     列族名称
    * @param fileNum    文件数量
    */
  def logFileNum(regionName: String, cfName: String, fileNum: Int): Unit = {

    //构造字符串数组，并用下划线连接起来，从而构造日志文件名称
    val stringArr = Arrays.asList("FileNum", regionName, cfName)
    val actualFileName = StringUtils.join(stringArr.toArray, '_')

    //如果文件已经存在，直接写入文件内容
    if (fileMap.containsKey(actualFileName)) {
      val out = fileMap.get(actualFileName)
      out.write("[" + System.currentTimeMillis() + "," + fileNum + "],\n")
      out.flush()
    } else {
      //如果文件不存在，先创建文件再写入文件内容
      val out = new FileWriter(outputBasePath + actualFileName , true)
      val fileName = actualFileName
      //fileMap += (actualFileName -> out)
      //fileMap += (actualFileName -> fileName)
      fileMap.put(actualFileName, out)
      out.write("[" + System.currentTimeMillis() + "," + fileNum + "],\n")
      out.flush()
    }

  }

  /**
    * 记录region对应列族所有文件大小
    *
    * @param regionName
    * @param cfName
    * @param fileTotalSize
    */

  def logFileTotalSizer(regionName: String, cfName: String, fileTotalSize: Long): Unit = {

    val stringArr = Arrays.asList("FileTotalSize", regionName, cfName)
    val actualFileName = StringUtils.join(stringArr.toArray, '_')

    //文件已经创建
    if (fileMap.containsKey(actualFileName)) {
      val out = fileMap.get(actualFileName)
      out.write("[" + System.currentTimeMillis() + "," + fileTotalSize / (1024 * 1024) + "],\n")
      out.flush()
    } else {

      val out = new FileWriter(outputBasePath + actualFileName , true)
      val fileName = actualFileName
      //fileMap += (actualFileName -> out)
      //fileMap += (actualFileName -> fileName)
      fileMap.put(actualFileName, out)
      out.write("[" + System.currentTimeMillis() + "," + fileTotalSize / (1024 * 1024) + "],\n")
      out.flush()
    }


  }


  /**
    * 获取region对应列族的信息
    *
    * @param regionName
    * @param familiyNames
    */
  def getRegionFileInfo(uri: String, tableDirName:String,regionName: String, familiyNames: java.util.ArrayList[String]): Unit = {
    //val dirname = "E:\\spark\\hbase-1.2.6\\store\\data\\default\\ly_test\\" + regionName + "\\0"

    try {

      import scala.collection.JavaConversions._
      for (familyName <- familiyNames) {

        val dirname = tableDirName + File.separator + regionName + File.separator + familyName
        val dir = new File(dirname)
        //val fileIter = subDir(dir)

        ///hbase/data/default/PHERF.USER_DEFINED_TEST/167704143b4abad814b3ec6d82df505e/0
        val hdfsFiles = hdfsSubDir(uri,dirname)
        var fileNum = 0
        var fileTotalSize: Long = 0

        //遍历列族在hdfs下的文件
        while (hdfsFiles.hasNext) {

          val file = new File(hdfsFiles.next())
          //获取文件大小
          val fileLength = getFileLength(StringUtils.replace(file.getPath,"hdfs:/","hdfs://")).toLong

          //每个文件输出单个文件的大小
          println("RegionName:" + regionName + "," + System.currentTimeMillis() + ",familyName:" + familyName + ",FileName:" + file.getName + ",FileSize:" + fileLength / (1024 * 1024))
          logFileSize(regionName, familyName, file.getName, fileLength)
          fileNum += 1
          fileTotalSize += fileLength
        }

        //每个列族输出列族文件数量
        println("RegionName:" + regionName + "," + System.currentTimeMillis() + ",familyName:" + familyName + ",FileNum:" + fileNum)
        logFileNum(regionName, familyName, fileNum)
        //每个列族输出所有文件的大小
        println("RegionName:" + regionName + "," + System.currentTimeMillis() + ",familyName:" + familyName + ",FileTotalSize:" + fileTotalSize)
        logFileTotalSizer(regionName, familyName, fileTotalSize)

      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        println(e.toString)
    }


  }


  /**
    * 非hdfs的访问文件列表的方法，包含递归访问多层子目录
    * @param dir 文件目录
    * @return 所有文件列表迭代器
    */
  def subDir(dir: File): Iterator[File] = {
    val dirs = dir.listFiles().filter(_.isDirectory())
    val files = dir.listFiles().filter(_.isFile())
    files.toIterator ++ dirs.toIterator.flatMap(subDir _)
  }

  /**
    * 获取HDFS的文件长度，File.length获取长度为0，通过下面方法获取
    * @param filePath 文件名在hdfs上的完全路径
    * @return 返回文件长度
    */
  def getFileLength(filePath:String):Long={
    fs.getFileStatus(new Path(filePath)).getLen
  }

  /**
    * 获取hdfs目录下所有文件的列表的迭代器
    * @param uri hdfs的uri，例如 hdfs://test1:8020/
    * @param dirName  hbase表在hdfs上的路径，例如 hbase/default/HERF.USER_DEFINED_TEST/167704143b4abad814b3ec6d82df505e/0
    * @return 目录下文件列表的迭代器
    */
  def hdfsSubDir(uri:String,dirName:String): Iterator[String] = {
    val lb = new ListBuffer[String]
    val lbChildFiles = HDFSHelper.listChildren(fs, dirName, lb)
    lbChildFiles.iterator
  }


}
