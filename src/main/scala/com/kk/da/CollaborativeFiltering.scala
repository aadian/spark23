package com.kk.da

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation._

/**
  * Created by kevin on 20/5/18.
  */
object CollaborativeFiltering {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().master("local").appName("Collaborative Filtering").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val rawUserArtistData = sc.textFile("/Develop/projects/spark23/src/main/resources/profiledata_06-May-2005/user_artist_data.txt")

    val rawArtistData = sc.textFile("/Develop/projects/spark23/src/main/resources/profiledata_06-May-2005/artist_data.txt")



    val artistByID = rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some(id.toInt, name.trim)
        } catch {
          case e: NumberFormatException => None

        }
      }

    }

    println("artist")
    artistByID.take(10).foreach(println)

    println(artistByID.lookup(6803336).head)
    println(artistByID.lookup(1000010).head)

    val rawArtisetAlias = sc.textFile("/Develop/projects/spark23/src/main/resources/profiledata_06-May-2005/artist_alias.txt")
    val artistAlias = rawArtisetAlias.flatMap(line => {
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some(tokens(0).toInt, tokens(1).toInt)
      }
    }).collectAsMap()

    println("artise alias")
    rawArtisetAlias.take(10).foreach(println)

    val bArtistAlias = sc.broadcast(artistAlias)

    val trainData = rawUserArtistData.map { line =>
      var Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)

    }.cache()


    val model = ALS.trainImplicit(trainData, 10, 5, 0.01 ,1.0)

    model.userFeatures.mapValues( _.mkString(", ")).first()

  }


}
