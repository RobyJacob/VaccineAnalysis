package VaccineAnalysis


object Main extends App {
  val analysis: Analysis = new Analysis();

//  analysis.vaccineCount.show();
  analysis.usDf.printSchema();
}
