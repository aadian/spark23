package com.kk.hbase

import java.io.{File, IOException}
import java.nio.file._
import java.util

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.commons.lang3.StringUtils


/**
  * @Auther: leiying
  * @Date: 2018/8/10 17:04
  * @Description:
  */


object HBaseAdmin {


  val conf = HBaseConfiguration.create()


  //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
  conf.set("hbase.zookeeper.quorum", "localhost")
  //设置zookeeper连接端口，默认2181
  conf.set("hbase.zookeeper.property.clientPort", "2181")

  def main(args: Array[String]): Unit = {

    //    val spark = SparkSession.builder().master("local").appName("HBase Admin").getOrCreate()
    //    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    //    val sc = new SparkContext(sparkConf)



    //createTable("ly_test","c")


    //    addRecord("ly_test","001","c","name","Kevin")
    //    addRecord("ly_test","002","c","name","Ivy")

    //    new ThreadReadHbase().start()
    //
//    new ThreadWriteHbase().start()
//    new ThreadGetAdminInfo().start()


    getHBaseConf

    //
    //    while (true) {
    //      getAdminInfo()
    //      Thread.sleep(500)
    //    }


    //getHBaseConf()


    //dropTable("ly_test")

    println("hello hbase")
  }

  /**
    * 获取hbase的配置
     */
  def getHBaseConf(): Unit = {
    val connection = ConnectionFactory.createConnection(conf);
    val hbaseConf = connection.getConfiguration
    println(hbaseConf.getStrings("hbase.hregion.memstore.flush.size")(0)) //134217728
    println(hbaseConf.getStrings("hbase.hregion.max.filesize")(0)) //10737418240
    println(hbaseConf.getStrings("hbase.regionserver.msginterval")(0)) //3000
    println(hbaseConf.getStrings("hbase.master.info.port")(0)) //16010
    println(hbaseConf.getStrings("hbase.regionserver.region.split.policy")(0)) //IncreasingToUpperBoundRegionSplitPolicy
    println(hbaseConf.getStrings("hbase.hregion.memstore.block.multiplier")(0)) //4
    println(hbaseConf.getStrings("hbase.hregion.memstore.flush.size")(0)) //4

    val tableDesc = new HTableDescriptor(TableName.valueOf("t3"));
    println("getMemStoreFlushSize:" + tableDesc.getMemStoreFlushSize)



    //println(hbaseConf.getStrings("hbase.regionserver.global.memstore.upperLimit")(0))
    //    println(hbaseConf.getStrings("hbase.hstore.compaction.min.size")(0))
    //    println(hbaseConf.getStrings("hbase.hregion.max.filesize")(0))

    //println(hbaseConf.getStrings("hbase.hstore.compaction.ratio")(0)) //10
    //println(hbaseConf.getStrings("hbase.increasing.policy.initial.size")(0)) //100


  }


  /**
    * 创建表，和列族，并设置表的Split Policy
    * @param tablename
    * @param columnFamily
    */
  def createTable(tablename: String, columnFamily: String): Unit = {

    val connection = ConnectionFactory.createConnection(conf);
    val admin = connection.getAdmin();
    val tableNameObj = TableName.valueOf(tablename);

    if (admin.tableExists(tableNameObj)) {
      System.out.println("Table exists!");
      System.exit(0);
    } else {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tablename));
      tableDesc.addFamily(new HColumnDescriptor(columnFamily));
      //tableDesc.setValue(HTableDescriptor.SPLIT_POLICY, "org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy")
      admin.createTable(tableDesc);
      println("create table success!");
    }

    admin.close();
    connection.close();

  }

  def getAdminInfo(): Unit = {
    val connection = ConnectionFactory.createConnection(conf);
    val admin = connection.getAdmin();

    import org.apache.hadoop.hbase.util.Bytes
    val clusterStatus = admin.getClusterStatus

    //    println("clusterStatus.getServersSize:" + clusterStatus.getServersSize)
    //    println("clusterStatus.getRegionsCount:" + clusterStatus.getRegionsCount)
    //    println("clusterStatus.getAverageLoad:" + clusterStatus.getAverageLoad)
    //    println("===========================")
    //    println()


    val serverIterator = clusterStatus.getServers.iterator()
    while (serverIterator.hasNext) {
      val serverName = serverIterator.next()
      //      println("server name:" + serverName)
      //      println("===========================")
      //      println()

      val serverLoad = clusterStatus.getLoad(serverName)
      //      println("server's getReadRequestsCount:" + serverLoad.getReadRequestsCount)
      //      println("server's getWriteRequestsCount:" + serverLoad.getWriteRequestsCount)
      //      println("server's getRequestsPerSecond:" + serverLoad.getRequestsPerSecond)
      //      println("===========================")
      //      println()

      val regLoadIt = serverLoad.getRegionsLoad.entrySet().iterator()
      while (regLoadIt.hasNext) {
        val regLoad = regLoadIt.next()
        val tableName = Bytes.toString(regLoad.getKey).split(",")(0)
        println("nothing")
        if (tableName.equalsIgnoreCase("ly_test")) {
          //println("tableName:" + tableName)
          println(System.currentTimeMillis() + "," + regLoad.getValue.getMemStoreSizeMB())

          //println("region:" + Bytes.toString(regLoad.getKey))
          //          println("region mem store size:" + regLoad.getValue.getMemStoreSizeMB())
          //          println("===========================")
          //          println()
        }

        //        println("region's getReadRequestsCount" + regLoad.getValue.getReadRequestsCount)
        //        println("region's getWriteRequestsCount" + regLoad.getValue.getWriteRequestsCount)
        //        println("region's getRequestsCount" + regLoad.getValue.getRequestsCount)
        //        println("===========================")
        //        println()
      }


    }

    //    for (serverName <- clusterStatus.getServers()) {
    //      val serverLoad = clusterStatus.getLoad(serverName)
    //      println("serverName:" + serverName)
    //      //      for (regionload <- serverLoad.getRegionsLoad.entrySet) {
    //      //        val regionName = Bytes.toString(regionload.)
    //      //        result.put(regionName, regionload.getValue)
    //      //      }
    //    }


  }


  def dropTable(tableName: String): Unit = {

    val connection = ConnectionFactory.createConnection(conf);
    val admin = connection.getAdmin();
    val table = TableName.valueOf(tableName);

    try {
      admin.disableTable(table)
      admin.deleteTable(table)
      println("delete table " + tableName + "ok.")
    } catch {
      case e: IOException => println(e)
    }

  }


  // 删除表
  //tableName: String, rowKey: String, family: String, qualifier: String, value: String
  def addRecord(): Unit = {

    val nameValue = "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
      "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111"


    try {
      val connection = ConnectionFactory.createConnection(conf)
      val table = connection.getTable(TableName.valueOf("ly_test"))

      for (i <- 1 to 200000) {
        val rowKey = StringUtils.rightPad("rowKey" + i, 12, "0")
        //HBaseAdmin.addRecord("ly_test", rowKey, "c", "name", nameValue)
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("name"), Bytes.toBytes(nameValue))
        put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("name"), Bytes.toBytes(nameValue))
        table.put(put)
        Thread.sleep(5)
      }


      table.close
      connection.close()
      //println("insert recored " + rowKey + " to table " + tableName + "ok.")
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

}

class ThreadWriteHbase() extends Thread {
  override def run() {
    HBaseAdmin.addRecord()
  }
}

class ThreadReadFile extends Thread {

  def subDir(dir: File): Iterator[File] = {
    val dirs = dir.listFiles().filter(_.isDirectory())
    val files = dir.listFiles().filter(_.isFile())
    files.toIterator ++ dirs.toIterator.flatMap(subDir _)
  }


  override def run(): Unit = {
    val dirname = "E:\\spark\\hbase-1.2.6\\store\\data\\default\\ly_test\\569ab525fd5591151801895106c431f6\\c"
    val dir = new File(dirname)

    while (true) {

      try {
        val fileIter = subDir(dir)
        var fileNum = 0
        var fileSize: Long = 0
        while (fileIter.hasNext) {
          val file = fileIter.next()
          println(file.getName + "," + file.length().toLong / (1024 * 1024))
          fileNum += 1
          fileSize += file.length().toLong / (1024 * 1024)
          //println("file Name:" + file.getName + ":" + file.length())
        }
        val currentTime = System.currentTimeMillis()
        println("file Number," + currentTime + "," + fileNum)
        println("file Accumumicate Size," + currentTime + "," + fileSize)

      } catch {
        case e: Exception => println(e.toString)
      }

      Thread.sleep(500)
    }


  }
}

class ThreadGetAdminInfo extends Thread {

  val conf = HBaseConfiguration.create()


  //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
  conf.set("hbase.zookeeper.quorum", "localhost")
  //设置zookeeper连接端口，默认2181
  conf.set("hbase.zookeeper.property.clientPort", "2181")

  val connection = ConnectionFactory.createConnection(conf);
  val admin = connection.getAdmin();


  override def run(): Unit = {


    while (true) {
      val clusterStatus = admin.getClusterStatus
      val serverIterator = clusterStatus.getServers.iterator()
      while (serverIterator.hasNext) {
        val serverName = serverIterator.next()
        val serverLoad = clusterStatus.getLoad(serverName)
        val regLoadIt = serverLoad.getRegionsLoad.entrySet().iterator()
        while (regLoadIt.hasNext) {
          val regLoad = regLoadIt.next()
          val tableName = Bytes.toString(regLoad.getKey).split(",")(0)
          if (tableName.equalsIgnoreCase("ly_test")) {
            val regionName = Bytes.toString(regLoad.getKey).split("\\.")(1)
            println("RegionName:"+ regionName + "," + System.currentTimeMillis() + ",MemStoreSize:" + regLoad.getValue.getMemStoreSizeMB())
            getRegionFileInfo(regionName)
          }
        }


      }

      Thread.sleep(500)
    }


  }

  def getRegionFileInfo(regionName: String): Unit = {
    val dirname = "E:\\spark\\hbase-1.2.6\\store\\data\\default\\ly_test\\" + regionName + "\\c"
    val dir = new File(dirname)

    try {
      val fileIter = subDir(dir)
      var fileNum = 0
      var fileTotalSize: Long = 0
      while (fileIter.hasNext) {
        val file = fileIter.next()
        println("RegionName:"+ regionName + "," + System.currentTimeMillis() + ",FileName:" + file.getName + ",FileSize:" + file.length().toLong / (1024 * 1024))
        fileNum += 1
        fileTotalSize += file.length().toLong / (1024 * 1024)
      }
      println("RegionName:"+ regionName + "," + System.currentTimeMillis() + ",FileNum:" + fileNum)
      println("RegionName:"+ regionName + "," + System.currentTimeMillis() + ",FileTotalSize:" + fileTotalSize)
    } catch {
      case e: Exception => println(e.toString)
    }
  }


  def subDir(dir: File): Iterator[File] = {
    val dirs = dir.listFiles().filter(_.isDirectory())
    val files = dir.listFiles().filter(_.isFile())
    files.toIterator ++ dirs.toIterator.flatMap(subDir _)
  }


}