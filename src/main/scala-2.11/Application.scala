import org.apache.log4j.Level
import org.apache.spark
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.col
import nk.util.SparkUtil._
/**
  * Created by NK on 2016. 9. 22..
  */
object Application {

  def main(args: Array[String]): Unit = {
    setLogLevel(Level.WARN)

    val spark = SparkSession
      .builder
      .appName("LogisticRegressionWithElasticNetExample")
      .master("local[*]")
      .getOrCreate()

    // $example on$
    // Load training data
//    val training = spark.read.format("libsvm").load("data/sample_libsvm_data.txt")
    val loanDF = spark.read
      .format("csv")
      .option("header", true)
      .csv("data/loan/2016Q2/LoanStats_2016Q2.csv")
//    val rejectDF = spark.read
//      .format("csv")
//      .option("header", "true")
//      .csv("data/loan/2016Q2/RejectStats_2016Q2.csv")

    loanDF.show(10)

    loanDF.select("loan_status").distinct().show(10)

//    Preprocess.run(loanDF)

    val training = spark.read.format("libsvm").load("data/2016Q2Feature-libsvm")
    training.show(30)
    training.rdd.map(row => row(1)).foreach(println)
//    val lr = new LogisticRegression()
//      .setMaxIter(10)
//      .setRegParam(0.03)
//      .setElasticNetParam(0.8)
//
//    // Fit the model
//    val lrModel = lr.fit(training)
//
//    // Print the coefficients and intercept for logistic regression
//    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    spark.stop()
  }
}
