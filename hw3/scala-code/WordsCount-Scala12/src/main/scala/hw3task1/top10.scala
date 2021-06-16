package hw3task1

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.io.Source
import java.io.File
import scala.collection.mutable.ListBuffer

object top10 {
  def main(args: Array[String]) {
    if (args.length != 2) {
      throw new IllegalArgumentException(
        "Exactly 2 arguments are required: <inputPath> <outputPath>")
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val sc = new SparkContext(new SparkConf()
      .setMaster("local")
      .setAppName("Top 10 Trending Videos"))

    println("The columns we have for each video:")
    println("video_id, trending_date, title, channel_title, category_id, publish_time, tags, views, likes, dislikes, comment_count, thumbnail_link, comments_disabled, ratings_disabled, video_error_or_removed, description")

    val starts_of_files = List("CA", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "RU", "US")
    val files = starts_of_files.map("/Users/sofiapetryshyn/IdeaProjects/WordsCount-Scala12/input_data/" + _ + "videos.csv").map(_.trim)

    // One file - one region data
    for {file <- files}
    {
      val lines = sc.textFile(file)
      val columnValues = lines.flatMap(line => line.split(","))
      val columnValues2 = columnValues.flatMap(colVals => colVals.take(0))
      val videoIdCounts = columnValues2.map(videoId => (videoId, 1)).reduceByKey(_ + _).sortBy(_._1)

      videoIdCounts
        .coalesce(1)
        .saveAsTextFile(outputPath)

    }
  }
}
