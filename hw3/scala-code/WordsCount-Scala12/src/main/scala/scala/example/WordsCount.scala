package scala.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    if (args.length != 2) {
      throw new IllegalArgumentException(
        "Exactly 2 arguments are required: <inputPath> <outputPath>")
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val sc = new SparkContext(new SparkConf()
      .setMaster("local")
      .setAppName("Words Count"))
    val lines = sc.textFile(inputPath)
    val words = lines.flatMap(line => line.split(" "))
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
    wordCounts
      .coalesce(1)
      .saveAsTextFile(outputPath)
  }
}
