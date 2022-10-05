import VaccineAnalysis.Analysis
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

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

  test("Analysis") {
    val analysis: Analysis = new Analysis();

    assert(analysis.usDf.schema == usDfSchema);
    assert(analysis.indDf.schema == indDfSchema);
    assert(analysis.ausDf.schema == ausDfSchema);
  }

  test("Analysis.vaccineCount") {
    val analysis : Analysis = new Analysis();

    val usDfData: Seq[Row] = Seq(
      Row(1, "Roby", "MNO", "12282021", "USA"),
      Row(2, "Rahul", "ABC", "06152022", "USA")
    );

    val indDfData: List[Row] = List(
      Row(1, "Mehana", "1998-12-01", "LPO", "2022-01-01", "F", "IND")
    );

    val ausDfData: List[Row] = List(
      Row(1, "Sameer", "MNO", "1952-08-13", "2022-02-20", "AUS"),
      Row(2, "Vikas", "ABC", "1998-12-01", "2022-03-05", "AUS")
    );

    val usDF: DataFrame = spark.createDataFrame(usDfData, usDfSchema);
  }
}
