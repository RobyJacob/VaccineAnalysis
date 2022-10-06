package VaccineAnalysis

import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.FileNotFoundException
import scala.reflect.io.File

class Analysis(basePath: String) {
  if (!File(basePath).exists)
    throw new FileNotFoundException(s"$basePath does not exist");

  private val spark: SparkSession = SparkSession.builder()
    .appName("VaccineAnalysis")
    .master("local[2]")
    .getOrCreate();

  private val usDfSchema : StructType = StructType(Array(
    StructField("Id", IntegerType, true),
    StructField("Name", StringType, true),
    StructField("VaccinationType", StringType, true),
    StructField("VaccinationDate", StringType, true),
    StructField("Country", StringType, false)
  ));

  private val indDfSchema : StructType = StructType(Array(
    StructField("Id", IntegerType, true),
    StructField("Name", StringType, true),
    StructField("Dob", StringType, true),
    StructField("VaccinationType", StringType, true),
    StructField("VaccinationDate", StringType, true),
    StructField("Free_or_paid", StringType, true)
  ));

  private val ausDfSchema : StructType = StructType(Array(
    StructField("Id", IntegerType, true),
    StructField("Name", StringType, true),
    StructField("VaccinationType", StringType, true),
    StructField("Dob", StringType, true),
    StructField("VaccinationDate", StringType, true)
  ));

  private var _usDf: DataFrame = {
    val path: String = s"$basePath/USA.csv";

    if (!File(path).exists)
      throw new FileNotFoundException(s"$path does not exist");

    spark.read
      .format("csv")
      .option("header", true)
      .schema(usDfSchema)
      .load(path)
      .withColumn("Country", lit("USA"))
  };

  private var _indDf: DataFrame = {
    val path: String = s"$basePath/IND.csv";

    if (!File(path).exists)
      throw new FileNotFoundException(s"$path does not exist");

    spark.read
      .format("csv")
      .option("header", true)
      .schema(indDfSchema)
      .load(path)
      .withColumn("Country", lit("IND"))
  };

  private var _ausDf: DataFrame = {
    val path: String = s"$basePath/AUS.xlsx";

    if (!File(path).exists)
      throw new FileNotFoundException(s"$path does not exist");

    spark.read
      .format("com.crealytics.spark.excel")
      .option("header", true)
      .schema(ausDfSchema)
      .load(path)
      .withColumn("Country", lit("AUS"))
  };

  def usDf: DataFrame = _usDf;

  def indDf: DataFrame = _indDf;

  def ausDf: DataFrame = _ausDf;

  def usDf_=(_val : DataFrame): Unit = {
    if (_val == null)
      throw new NullPointerException("Input is null");

    _usDf = _val;
  };

  def indDf_=(_val : DataFrame): Unit = {
    if (_val == null)
      throw new NullPointerException("Input is null");

    _indDf = _val;
  };

  def ausDf_=(_val : DataFrame): Unit = {
    if (_val == null)
      throw new NullPointerException("Input is null");

    _ausDf = _val;
  };

  def run: DataFrame = {
    val fullDf: DataFrame = _usDf.select("Country", "VaccinationType")
      .union(_indDf.select("Country", "VaccinationType"))
      .union(_ausDf.select("Country", "VaccinationType"));

    fullDf.createOrReplaceTempView("vaccine_analysis");

    spark.sql(
      """
        |SELECT
        |Country,
        |VaccinationType,
        |COUNT(*) OVER(PARTITION BY Country,VaccinationType) AS NoOfVaccinations,
        |(COUNT(*) OVER(PARTITION BY Country)) * 100 / COUNT(*) OVER() AS PercentageVaccinated,
        |(COUNT(*) OVER(PARTITION BY Country,VaccinationType)) * 100 / COUNT(*) OVER() AS PercentageVaccineContribution
        |FROM vaccine_analysis
        |""".stripMargin).distinct();
  }
}
