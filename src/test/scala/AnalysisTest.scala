import VaccineAnalysis.Analysis
import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.io.FileNotFoundException

class AnalysisTest extends AnyFunSuite{
  val spark: SparkSession = SparkSession.builder()
    .appName("VaccineAnalysis")
    .master("local[2]")
    .getOrCreate();

  val usDfSchema: StructType = StructType(Array(
    StructField("Id", IntegerType, true),
    StructField("Name", StringType, true),
    StructField("VaccinationType", StringType, true),
    StructField("VaccinationDate", StringType, true),
    StructField("Country", StringType, false)
  ));

  val indDfSchema: StructType = StructType(Array(
    StructField("Id", IntegerType, true),
    StructField("Name", StringType, true),
    StructField("Dob", StringType, true),
    StructField("VaccinationType", StringType, true),
    StructField("VaccinationDate", StringType, true),
    StructField("Free_or_paid", StringType, true),
    StructField("Country", StringType, false)
  ));

  val ausDfSchema : StructType = StructType(Array(
    StructField("Id", IntegerType, true),
    StructField("Name", StringType, true),
    StructField("VaccinationType", StringType, true),
    StructField("Dob", StringType, true),
    StructField("VaccinationDate", StringType, true),
    StructField("Country", StringType, false)
  ));

  val crctBasePath: String = "input";

  test("Analysis") {
    val wrongBasePath: String = "../../../../input";

    assertThrows[FileNotFoundException] (new Analysis(wrongBasePath));

    val analysis: Analysis = new Analysis(crctBasePath);

    assertThrows[NullPointerException] (analysis.usDf = null);
    assertThrows[NullPointerException] (analysis.indDf = null);
    assertThrows[NullPointerException] (analysis.ausDf = null);

    assert(analysis.usDf.schema == usDfSchema);
    assert(analysis.indDf.schema == indDfSchema);
    assert(analysis.ausDf.schema == ausDfSchema);
  }

  test("Analysis.run") {
    val analysis: Analysis = new Analysis(crctBasePath);

    val usDfData: List[Row] = List(
      Row(1, "Roby", "MNO", "12282021", "USA"),
      Row(2, "Rahul", "ABC", "06152022", "USA")
    );

    val indDfData: List[Row] = List(
      Row(1, "Mehana", "1998-12-01", "LPO", "2022-01-01", "F", "IND")
    );

    val ausDfData: List[Row] = List(
      Row(1, "Sameer", "MNO", "1952-08-13", "2022-02-20", "AUS"),
      Row(2, "Vikas", "MNO", "1998-12-01", "2022-03-05", "AUS")
    );

    val usDF: DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(usDfData), usDfSchema);

    val indDf: DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(indDfData), indDfSchema);

    val ausDf: DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(ausDfData), ausDfSchema);

    analysis.usDf = usDF;
    analysis.indDf = indDf;
    analysis.ausDf = ausDf;

    val resDfTmp: DataFrame = usDF.select("Country", "VaccinationType")
      .union(indDf.select("Country", "VaccinationType"))
      .union(ausDf.select("Country", "VaccinationType"));

    resDfTmp.createOrReplaceTempView("analysis");

    val resDf: DataFrame = spark.sql(
      """
        |SELECT
        |Country,
        |VaccinationType,
        |COUNT(*) OVER(PARTITION BY Country,VaccinationType) AS NoOfVaccinations,
        |(COUNT(*) OVER(PARTITION BY Country)) * 100 / COUNT(*) OVER() AS PercentageVaccinated,
        |(COUNT(*) OVER(PARTITION BY Country,VaccinationType)) * 100 / COUNT(*) OVER() AS PercentageVaccineContribution
        |FROM analysis
        |""".stripMargin).distinct();

    assert(analysis.run.collectAsList() == resDf.collectAsList());
  }
}
