import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row}

import scala.math._
import scala.util.matching.Regex

/**
  * Created by NK on 2016. 10. 6..
  */
object Preprocess {
  val MONTHLY = 12

  case class Feature(loanStatus:Double,
                     annualIncome:Double,
                     creditAge:Double,
                     delinq:Double,
                     employLen:Double,
                     homeOwner:Double,
                     inquiries:Double,
                     loanAmount:Double,
                     purpose:Double,
                     openAcc:Double,
                     totalAcc:Double,
                     term:Double,
                     dti:Double,
                     itp:Double,
                     revolUtil:Double,
                     rti:Double
                    )
  def run(loanDF:DataFrame): Unit = {
    // For implicit conversions from RDDs to DataFrames

    val preDataDF = loanDF.select(
      loanDF("loan_status"),                    // 0
      loanDF("annual_inc").cast("double"),      // 1
      loanDF("earliest_cr_line"),               // 2
      loanDF("delinq_2yrs").cast("int"),        // 3
      loanDF("emp_length"),                     // 4
      loanDF("home_ownership").cast("string"),  // 5
      loanDF("inq_last_6mths").cast("int"),     // 6
      loanDF("loan_amnt").cast("double"),       // 7
      loanDF("purpose").cast("string"),         // 8
      loanDF("open_acc").cast("double"),        // 9
      loanDF("total_acc").cast("double"),       // 10
      loanDF("term").cast("string"),            // 11
      loanDF("dti").cast("double"),             // 12
      loanDF("installment").cast("double"),     // 13
      loanDF("revol_util").cast("string"),      // 14
      loanDF("revol_bal").cast("double"))       // 15

    preDataDF.printSchema()
    preDataDF.show(5)
    preDataDF.select("term").distinct().show(100)

//    preDataDF.limit(5).rdd.map(row => calculateITP(row(13).toString, row(1).toString)).foreach(println)
    val featureRow = preDataDF.rdd.map(manipulateData)

    // Prepare training data from a list of (label, features) tuples.


    val featureDF = preDataDF.sqlContext.createDataFrame(featureRow).toDF("label", "features")
    featureDF.show(5)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.03)

    // Fit the model
    val lrModel = lr.fit(featureDF)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
//    featureDF.coalesce(1).write.option("header",true).format("csv").save("feature_output")
  }

  def manipulateData(row:Row) = {
//    val loan_status = row(0)
//    val annual_inc = row(1).toString.toDouble
//    val credit_age = row(2).toString
//    val deliq = row(3).toString.toInt
//    val emp_length = row(4).toString
//    val home_ownership = row(5).toString
//    val inq_last_6mths = row(6).toString.toInt
//    val loan_amnt = row(7).toString.toDouble
//    val purpose = row(8).toString
//    val open_acc = row(9).toString.toDouble
//    val total_acc = row(10).toString.toDouble
//    val term = row(11).toString
//    val dti = row(12).toString.toDouble
//    val installment = row(13).toString.toDouble   // The monthly payment owed by the borrower if the loan originates.
//    val revol_util = row(14).toString
//    val revol_bal = row(15).toString.toDouble     // Total credit revolving balance

    val loan_status = row(0)
    val annual_inc = row(1)
    val credit_age = row(2)
    val deliq = row(3)
    val emp_length = row(4)
    val home_ownership = row(5)
    val inq_last_6mths = row(6)
    val loan_amnt = row(7)
    val purpose = row(8)
    val open_acc = row(9)
    val total_acc = row(10)
    val term = row(11)
    val dti = row(12)
    val installment = row(13)     // The monthly payment owed by the borrower if the loan originates.
    val revol_util = row(14)
    val revol_bal = row(15)       // Total credit revolving balance

    var new_loan_status = 0.0
    var new_annual_inc = 0.0
    var new_credit_age = 0.0
    var new_deliq = 0
    var new_emp_length = 0
    var new_home_ownership = 0
    var new_inq_last_6mths = 0
    var new_loan_amnt = 0.0
    var new_purpose = 0
    var new_open_acc = 0.0
    var new_total_acc = 0.0
    var new_term = 0
    var new_dti = 0.0
    var new_itp = 0.0            // Ratio of the loan’s monthly payments to monthly income.
    var new_revol_util = 0.0
    var new_rti = 0.0

    if(loan_status.equals("Fully Paid")){
      new_loan_status = 1.0
    }else{
      new_loan_status = 0.0
    }

    new_annual_inc = calculateAnnualIncome(annual_inc)
    new_credit_age = calculateCreditAge(credit_age)
    new_deliq = calculateDeliq(deliq)
    new_emp_length = calculateEmployLength(emp_length)
    new_home_ownership = calculateHomeOwnership(home_ownership)
    new_inq_last_6mths = calculateInquiries(inq_last_6mths)
    new_loan_amnt = transferNullToDoubleValue(loan_amnt)
    new_purpose = calculatePurpose(purpose)
    new_open_acc = transferNullToDoubleValue(open_acc)
    new_total_acc = transferNullToDoubleValue(total_acc)
    new_term = calculateTerm(term)
    new_dti = transferNullToDoubleValue(dti)
    new_itp = calculateITP(transferNullToDoubleValue(installment), transferNullToDoubleValue(annual_inc))
    new_revol_util = calculateRevolUtil(revol_util)
    new_rti = calculateRTI(transferNullToDoubleValue(revol_bal), transferNullToDoubleValue(annual_inc))

    (
      new_loan_status,
      Vectors.dense(
        new_annual_inc,
        new_credit_age,
        new_deliq,
        new_emp_length,
        new_home_ownership,
        new_inq_last_6mths,
        new_loan_amnt,
        new_purpose,
        new_open_acc,
        new_total_acc,
        new_term,
        new_dti,
        new_itp,
        new_revol_util,
        new_rti
      )
    )

  }
  def transferNullToIntValue(param:Any) = {
    if(param != null){
      param.toString.toInt
    }else{
      0
    }
  }

  def transferNullToDoubleValue(param:Any) = {
    if(param != null){
      param.toString.toDouble
    }else{
      0.0
    }
  }
  def calculateInquiries(inq_last_6mths:Any) = {
    transferNullToIntValue(inq_last_6mths)
  }

  def calculateDeliq(deliq:Any) = {
    transferNullToIntValue(deliq)
  }

  def calculateAnnualIncome(annual_inc:Any) = {
    val ann_inc = transferNullToDoubleValue(annual_inc)
    val rslt = log(ann_inc)
    if(rslt.isInfinity){
      0.0
    }else{
      rslt
    }
  }
  /**
    * Credit Line이 최초로 보고 된 날부터 현재까지의 개월 수 (Natural logarithm로 값 변경)
    *
    * @param earliest_cr_line
    * @return
    */
  def calculateCreditAge(earliest_cr_line:Any) = {

    if(earliest_cr_line == null || earliest_cr_line.equals("")){
      0.0
    }else{
      val date = earliest_cr_line.toString.split("\\.")
      if(date.length == 3){ // exception
        val startDate = LocalDate.of(date(0).toInt, date(1).toInt, date(2).toInt)
        val formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd")

        val oldDate = startDate
        val currentDate = "2016.12.25"
        val newDate = LocalDate.parse(currentDate, formatter)
        val diffDay = newDate.toEpochDay() - oldDate.toEpochDay()

        val reslt = math.log(diffDay/365)
        if(reslt.isInfinity){
          0.0
        }else{
          reslt
        }
      }else{
        0.0
      }

    }

  }

  /**
    * 근무 년식. 0과 10 사이에 수로 표현. 0은 1년 미만, 10은 10년이상
    *
    * @param emp_length
    */
  def calculateEmployLength(emp_length:Any) = {
    if(emp_length != null){
      val emp_len = emp_length.toString
      if(emp_len.equals("< 1 year") || emp_len.equals("n/a") || emp_len.length < 2){
        0
      }else{
        val pattern = new Regex("\\d+")
        val len = pattern.findAllIn(emp_len).matchData.next()
        //      println(s"$emp_len || [$len] ")

        len.toString().toInt
      }
    }else{
      0
    }
  }

  /**
    * 대출자의 집 보유 상태. Rent, Own, 그리고 Mortgage 중에 하나의 값을 가짐
    * Own은 1, 나머지는 0.
    *
    * @param home_ownership
    * @return
    */
  def calculateHomeOwnership(home_ownership:Any) = {
    if(home_ownership != null){
      home_ownership.toString match {
        case "OWN" => 1
        case _ => 0
      }
    }else{
      -1
    }

  }

  /**
    *
    * @param revol_util
    * @return
    */
  def calculateRevolUtil(revol_util:Any) = {
    try{
      val pattern = new Regex("\\d+")
      val percent = pattern.findAllIn(revol_util.toString).matchData.next().toString().toDouble
      percent / 100.0
    }catch {
      case nse : NoSuchElementException => {
        0.0
      }
    }

  }
  /**
    *
    * @param purpose
    * @return
    */
  def calculatePurpose(purpose:Any) = {
    purpose.toString match {
      case "small_business" => 0
      case "debt_consolidation" => 1
      case "credit_card" => 2
      case "moving" => 3
      case "vacation" => 4
      case "renewable_energy" => 5
      case "house" => 6
      case "car" => 7
      case "major_purchase" => 8
      case "medical" => 9
      case "home_improvement" => 10
      case _ => -1
    }
  }
  def calculateTerm(term:Any) = {
    if(term !=  null){
      if(term.toString.length < 2){
        0
      }else{
        val pattern = new Regex("\\d+")
        val len = pattern.findAllIn(term.toString).matchData.next()
        //      println(s"$term || [$len] ")

        len.toString().toInt
      }
    }else{
      0
    }

  }

  /**
    * Ratio of the loan’s monthly payments to monthly income.
    *
    * @param installment  The monthly payment owed by the borrower if the loan originates.
    * @param annual_inc   연간 수입.
    * @return pti
    */
  def calculateITP(installment:Double, annual_inc:Double) = {
    val mon_pay = installment
    val ann_inc = annual_inc

    val mon_inc = ann_inc / MONTHLY

    mon_pay / mon_inc
  }

  /**
    * Ratio of revolving credit balance to the borrower’s monthly income.
    *
    * @param revol_bal
    * @param annual_inc
    * @return
    */
  def calculateRTI(revol_bal:Double, annual_inc:Double) = {
    val rev_bal = revol_bal
    val mon_inc = annual_inc / MONTHLY

    rev_bal / mon_inc
  }
}
