package edu.neu.coe.csye7200.asstmd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter

class MovieAnalyzerSpec extends AnyFunSuite with BeforeAndAfter {
  var spark: SparkSession = _

  before {
    spark = SparkSession.builder()
      .appName("MovieRatingAnalyzerTest")
      .master("local[*]")
      .getOrCreate()
  }

  after {
    if (spark != null) spark.stop()
  }

  def createTestDataFrame(data: Seq[(String, Double)]): org.apache.spark.sql.DataFrame = {
    val testDF = spark.createDataFrame(data).toDF("movieId", "rating")
    testDF
  }

  test("Mean rating calculation for single movie") {
    val testData = Seq(("1", 5.0), ("1", 3.0))
    val testDF = createTestDataFrame(testData)
    val resultDF = MovieAnalyzer.analyzeRatings(testDF)
    val meanRating = resultDF.collect()(0).getAs[Double]("mean_rating")
    assert(meanRating === 4.0)
  }

  test("Mean rating calculation with diverse ratings") {
    val testData = Seq(("2", 2.0), ("2", 4.0), ("2", 6.0))
    val testDF = createTestDataFrame(testData)
    val resultDF = MovieAnalyzer.analyzeRatings(testDF)
    val meanRating = resultDF.collect()(0).getAs[Double]("mean_rating")
    assert(meanRating === 4.0)
  }

  test("Mean rating calculation with identical ratings") {
    val testData = Seq(("3", 5.0), ("3", 5.0), ("3", 5.0))
    val testDF = createTestDataFrame(testData)
    val resultDF = MovieAnalyzer.analyzeRatings(testDF)
    val meanRating = resultDF.collect()(0).getAs[Double]("mean_rating")
    assert(meanRating === 5.0)
  }

  test("Standard deviation calculation for single movie with diverse ratings") {
    val testData = Seq(("4", 1.0), ("4", 2.0), ("4", 3.0))
    val testDF = createTestDataFrame(testData)
    val resultDF = MovieAnalyzer.analyzeRatings(testDF)
    val stdDevRating = resultDF.collect()(0).getAs[Double]("std_dev_rating")
    assert(stdDevRating === 1.0)
  }

  test("Standard deviation calculation with identical ratings") {
    val testData = Seq(("5", 4.0), ("5", 4.0), ("5", 4.0))
    val testDF = createTestDataFrame(testData)
    val resultDF = MovieAnalyzer.analyzeRatings(testDF)
    val stdDevRating = resultDF.collect()(0).getAs[Double]("std_dev_rating")
    assert(stdDevRating === 0.0)
  }

  test("Standard deviation calculation for single movie with varying ratings") {
    val testData = Seq(("6", 2.0), ("6", 4.0), ("6", 4.0), ("6", 4.0), ("6", 5.0), ("6", 5.0), ("6", 5.0), ("6", 7.0))
    val testDF = createTestDataFrame(testData)
    val resultDF = MovieAnalyzer.analyzeRatings(testDF)
    val stdDevRating = resultDF.collect()(0).getAs[Double]("std_dev_rating")
    assert(stdDevRating > 0)
  }
  test("Handling of negative and zero ratings in dataset") {
    val testData = Seq(("8", -1.0), ("8", 0.0), ("8", 5.0), ("8", 10.0))
    val testDF = createTestDataFrame(testData)
    val resultDF = MovieAnalyzer.analyzeRatings(testDF)

    val meanRating = resultDF.collect()(0).getAs[Double]("mean_rating")
    val stdDevRating = resultDF.collect()(0).getAs[Double]("std_dev_rating")

    assert(meanRating > 0.0 && meanRating < 5.0, "Mean rating should be positively skewed due to negative and zero ratings")
    assert(stdDevRating > 0.0, "Standard deviation should account for the spread including negative and zero ratings")
  }
}

