package VaccineAnalysis

import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession};

class Analysis {
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

  private var _usDf: DataFrame = spark.read
    .format("csv")
    .schema(usDfSchema)
    .load("/Users/robyjacob/Downloads/Incubyte DE Assessment/SampleInputData/USA.csv")
    .withColumn("Country", lit("USA"));

  private var _indDf: DataFrame = spark.read
    .format("csv")
    .schema(indDfSchema)
    .load("/Users/robyjacob/Downloads/Incubyte DE Assessment/SampleInputData/IND.csv")
    .withColumn("Country", lit("IND"));

  private var _ausDf: DataFrame = spark.read
    .format("com.crealytics.spark.excel")
    .option("header", true)
    .schema(ausDfSchema)
    .load("/Users/robyjacob/Downloads/Incubyte DE Assessment/SampleInputData/AUS.xlsx")
    .withColumn("Country", lit("AUS"));

  def usDf: DataFrame = _usDf;

  def indDf: DataFrame = _indDf;

  def ausDf: DataFrame = _ausDf;

  def usDf_=(_val : DataFrame): Unit = _usDf = _val;

  def indDf_=(_val : DataFrame): Unit = _indDf = _val;

  def ausDf_=(_val : DataFrame): Unit = _ausDf = _val;

  def vaccineCount: DataFrame =
    _usDf.select("Country", "VaccinationType")
      .union(_indDf.select("Country", "VaccinationType"))
      .union(_ausDf.select("Country", "VaccinationType"))
      .groupBy("Country", "VaccinationType")
      .agg(count(lit(1)).alias("VaccineCount"));

  def vaccinatedPerc: DataFrame =
    val fullDf: DataFrame = _usDf.select("Country")
      .union(_indDf.select("Country"))
      .union(_ausDf.select("Country"));

    val totalPopulation: Long = fullDf.count();

    fullDf.groupBy("Country")
      .agg((count(lit(1)) * 100 / totalPopulation).alias("PercentageVaccinated"));
}
