package edu.neu.coe.csye7200.asstmd

import scala.io.Source
import scala.util._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object MovieAnalyzer extends App {
  val spark: SparkSession = SparkSession.builder()
    .appName("MovieRatingAnalyzer")
    .master("local[*]") // Use local[*] for development/testing
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // Path to your movie ratings CSV file
  val movieDataPath = "/Users/khursheed/IdeaProjects/CSYE7200/assignment-movie-database/src/test/resources/movie_metadata.csv"

  // Function to read the CSV file into a DataFrame
  def readMovieData(path: String): DataFrame = {
    spark.read.option("header", "true").option("inferSchema", "true").csv(path)
  }

  // Function to analyze movie ratings
  def analyzeRatings(movieDF: DataFrame): DataFrame = {
    movieDF.groupBy("movieId")
      .agg(
        mean("rating").alias("mean_rating"),
        stddev("rating").alias("std_dev_rating")
      )
  }

  // Main workflow
  val movieDF = readMovieData(movieDataPath)
  val analysisResults = analyzeRatings(movieDF)
  analysisResults.show(truncate = false)
}
