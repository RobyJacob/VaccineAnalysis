package VaccineAnalysis

import org.apache.spark.sql.DataFrame


object Main extends App {
  val basePath: String = "input";

  val analysis: Analysis = new Analysis(basePath);

  val resDf: DataFrame = analysis.run;

  resDf.write
    .format("csv")
    .mode("overwrite")
    .save("output")
}

