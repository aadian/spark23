package com.kk.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes

/**
  * 获取HBase的管理信息
  */
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
            println("MemStoreSize," + System.currentTimeMillis() + "," + regLoad.getValue.getMemStoreSizeMB())
          }
        }
      }
      Thread.sleep(500)
    }

  }


}
