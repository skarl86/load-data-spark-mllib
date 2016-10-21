import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{Row, DataFrame}
import scala.math._
import scala.util.matching.Regex

/**
  * Created by NK on 2016. 10. 6..
  */
object Preprocess {
  val MONTHLY = 12

  case class RawData(loanStatus:Boolean,
                     annualIncome:Double,
                     creditAge:String,
                     delinq:Int,
                     employLen:String,
                     homeOwner:String,
                     inquiries:Int,
                     loanAmount:Double,
                     purpose:String,
                     openAcc:Double,
                     totalAcc:Double,
                     term:String,
                     dti:Double,
                     itp:Double,
                     revolUtil:String,
                     rit:Double
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

    preDataDF.limit(5).rdd.map(row => calculatePTI(row(13).toString, row(1).toString)).foreach(println)
  }

  def manipulateData(row:Row) = {
    val loan_status = row(0)
    val annual_inc = row(1).toString.toDouble
    val credit_age = row(2).toString
    val deliq = row(3).toString.toInt
    val emp_length = row(4).toString
    val home_ownership = row(5)
    val inq_last_6mths = row(6)
    val loan_amnt = row(7)
    val purpose = row(8)
    val open_acc = row(9)
    val total_acc = row(10)
    val term = row(11)
    val dti = row(12)
    val installment = row(13)   // The monthly payment owed by the borrower if the loan originates.
    val revol_util = row(14)
    val revol_bal = row(15)     // Total credit revolving balance

    var new_loan_status = false
    var new_annual_inc = 0.0
    var new_credit_age = 0.0
    var new_deliq = 0
    var new_emp_length = 0
    var new_home_ownership = ""
    var new_inq_last_6mths = 0
    var new_loan_amnt = 0
    var new_purpose = ""
    var new_open_acc = 0
    var new_total_acc = 0
    var new_term = 0
    var new_dti = 0.0
    var new_itp = 0             // Ratio of the loan’s monthly payments to monthly income.
    var new_revol_util = 0.0
    var new_revol_bal = 0.0

    if(loan_status.equals("Fully Paid")){
      new_loan_status = true
    }else{
      new_loan_status = false
    }

    new_annual_inc = log(annual_inc)
    new_credit_age = calculateCreditAge(credit_age)
    new_deliq = deliq
    new_emp_length = calculateEmployLength(emp_length)

  }

  /**
    * Credit Line이 최초로 보고 된 날부터 현재까지의 개월 수 (Natural logarithm로 값 변경)
    *
    * @param earliest_cr_line
    * @return
    */
  def calculateCreditAge(earliest_cr_line:String) = {

    val date = earliest_cr_line.split("\\.")
    val startDate = LocalDate.of(date(0).toInt, date(1).toInt, date(2).toInt)
    val formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd")

    val oldDate = startDate
    val currentDate = "2016.12.25"
    val newDate = LocalDate.parse(currentDate, formatter)
    val diffDay = newDate.toEpochDay() - oldDate.toEpochDay()

    math.log(diffDay/365)
  }

  /**
    * 근무 년식. 0과 10 사이에 수로 표현. 0은 1년 미만, 10은 10년이상
    *
    * @param emp_length
    */
  def calculateEmployLength(emp_length:String) = {
    if(emp_length.equals("< 1 year") || emp_length.equals("n/a") || emp_length.length < 2){
      0
    }else{
      val pattern = new Regex("\\d+")
      val len = pattern.findAllIn(emp_length).matchData.next()
      println(s"$emp_length || [$len] ")

      len.toString().toInt
    }
  }

  /**
    * 대출자의 집 보유 상태. Rent, Own, 그리고 Mortgage 중에 하나의 값을 가짐
    * Own은 1, 나머지는 0.
    * @param home_ownership
    * @return
    */
  def calculateHomeOwnership(home_ownership:String) = {
    println(s"$home_ownership")
    home_ownership match {
      case "OWN" => 1
      case _ => 0
    }
  }

  def calculatePurpose(purpose:String) = {
    purpose match {
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
  def calculateTerm(term:String) = {
    if(term.length < 2){
      0
    }else{
      val pattern = new Regex("\\d+")
      val len = pattern.findAllIn(term).matchData.next()
      println(s"$term || [$len] ")

      len.toString().toInt
    }
  }

  /**
    * Ratio of the loan’s monthly payments to monthly income.
    * @param installment  The monthly payment owed by the borrower if the loan originates.
    * @param annual_inc   연간 수입.
    * @return pti
    */
  def calculatePTI(installment:String, annual_inc:String) = {
    val mon_pay = installment.toDouble
    val ann_inc = annual_inc.toDouble

    val mon_inc = ann_inc / MONTHLY

    mon_pay / mon_inc
  }

  /**
    * Ratio of revolving credit balance to the borrower’s monthly income.
    */
  def calculateRTI(revol_bal:String, annual_inc:String) = {
    val rev_bal = revol_bal.toDouble
    val mon_inc = annual_inc.toDouble / MONTHLY

    rev_bal / mon_inc
  }
}
