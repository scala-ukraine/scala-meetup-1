package com.github.scalaukraine

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.sql.Timestamp

object Main {
  case class Video(
    id:          String,
    title:       String,
    publishedAt: Timestamp,
    keyword:     String)

  case class Comment(
    videoId:   String,
    likes:     Int,
    sentiment: Int,
    comment:   String)

  case class VideoStats(
    id:            String,
    title:         String,
    publishedAt:   Timestamp,
    keyword:       String,
    comments:      Int,
    sentimentRate: Int
    // TODO: uncomment after evolution
//    newColumn: Option[Boolean]
  )

  def main(args: Array[String]): Unit =
    withSparkSession { implicit spark =>
      import spark.implicits._

      val videos = readVideos
      videos.show(truncate = false)
      pause("This is how videos look like")

      val comments = readComments
      comments.show(truncate = false)
      pause("This is how comments look like")

      initialPopulation(videos)

      populateWithComments()

      populateWithComments()

      timeTravel()

      changeFeed()

      optimize()

      schemaEvolution()
    }

  private def initialPopulation(videos: Dataset[Video])(implicit spark: SparkSession): Unit =
    withVideoStats { videoStatsDeltaTable =>
      import spark.implicits._
      val newVideos = videos
        .as("new")
        .withColumn("comments", lit(0))
        .withColumn("sentimentRate", lit(0))

      videoStatsDeltaTable
        .as("existing")
        .merge(newVideos, $"existing.id" === $"new.id")
        .whenNotMatched()
        .insertAll()
        .execute()

      printDeltaSchema()
      showDeltaTable()
      pause("Look at initially populated delta table")
    }

  private def populateWithComments()(implicit spark: SparkSession): Unit =
    withVideoStats { videoStatsDeltaTable =>
      import spark.implicits._

      val newStats = readComments
        .toDF()
        .groupBy($"videoId")
        .agg(
          count($"videoId").as("comments"),
          sum(
            when(
              $"sentiment" === lit(1),
              lit(0)
            ).otherwise(
              when(
                $"sentiment" === lit(0),
                lit(-1)
              ).otherwise(lit(1))
            )
          ).as("sentimentRate")
        )
        .as("updates")

      videoStatsDeltaTable
        .as("stats")
        .merge(newStats, $"stats.id" === $"updates.videoId")
        .whenMatched()
        .update(
          Map(
            "comments"      -> ($"stats.comments" + $"updates.comments"),
            "sentimentRate" -> ($"stats.sentimentRate" + $"updates.sentimentRate")
          )
        )
        .execute()

      showDeltaTable()
      pause("Look at the updated delta table")
    }

  private def withSparkSession(thunk: SparkSession => Unit): Unit = {
    val spark = SparkSession
      .builder()
      .appName("deltalake")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    try thunk(spark)
    finally spark.close()
  }

  private def timeTravel()(implicit spark: SparkSession): Unit = {
    spark.read
      .format("delta")
      .option("versionAsOf", "1")
      .load("spark-warehouse/video-stats")
      .show(truncate = false)

    pause("Look at the first version of the table!")
  }

  private def schemaEvolution()(implicit spark: SparkSession): Unit = {
    withVideoStats { _ =>
      spark.sql(
        """
        ALTER TABLE video_stats
        ADD COLUMN newColumn BOOLEAN;
        """
      )
    }

    printDeltaSchema()
    pause("Look at the new schema")

    showDeltaTable()
    pause("Updated table")
  }

  private def optimize()(implicit spark: SparkSession): Unit = {
    triggerDeltaRegistrationInSpark()
    spark.sql(
      """
        OPTIMIZE video_stats ZORDER BY (publishedAt)
        """
    )
  }

  private def changeFeed()(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    triggerDeltaRegistrationInSpark()

    val feed = spark.read
      .format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", 0)
      .table("video_stats")

    feed.show(truncate = false)

    pause("Look at change feed")

    feed
      .where($"_change_type".isin("insert", "update_postimage", "delete"))
      .groupBy($"id", $"_change_type")
      .agg(count($"_change_type").as("count"))
      .show(truncate = false)

    pause("Changes stats")
  }

  private def printDeltaSchema()(implicit spark: SparkSession): Unit =
    withVideoStats(_.toDF.printSchema())

  private def showDeltaTable()(implicit spark: SparkSession): Unit =
    withVideoStats(_.toDF.show(truncate = false))

  private def withVideoStats(thunk: DeltaTable => Unit)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    thunk {
      DeltaTable
        .createIfNotExists(spark)
        .tableName("video_stats")
        .location("video-stats")
        .addColumns(structTypeOf[VideoStats])
        .property("delta.enableChangeDataFeed", "true")
        // TODO: uncomment to see partitioning!
//        .partitionedBy("keyword")
        .execute()
    }
  }

  private def readVideos(implicit spark: SparkSession): Dataset[Video] = {
    import spark.implicits._
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/videos-stats.csv")
      .select(
        $"Video ID".as("id"),
        $"Title".as("title"),
        $"Published At".as("publishedAt").cast(TimestampType),
        $"Keyword".as("keyword")
      )
      .as[Video]
  }

  private def readComments(implicit spark: SparkSession): Dataset[Comment] = {
    import spark.implicits._
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("multiLine", "true")
      .option("escape", "\"")
      .csv("data/comments.csv")
      .select(
        $"Video ID".as("videoId"),
        $"Likes".as("likes").cast(IntegerType),
        $"Sentiment".as("sentiment").cast(IntegerType),
        $"Comment".as("comment")
      )
      .as[Comment]
  }

  private def structTypeOf[A: Encoder]: StructType = implicitly[Encoder[A]].schema

  private def pause(message: String): Unit =
    scala.io.StdIn.readLine(message + "\n")

  private def triggerDeltaRegistrationInSpark()(implicit spark: SparkSession): Unit =
    withVideoStats { _ =>
      println("Regisetred delta table in spark catalog")
    }
}
