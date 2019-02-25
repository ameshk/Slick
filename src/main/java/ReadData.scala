import org.apache.spark.sql.SparkSession

class ReadData(spark: SparkSession, wareHouseLocation: String) {
  def readIndexData(tableName: String, readObj: ReadEncapsObj, outputTableName:String) {
    val primaryKeyColumn = readObj.primaryKeyColumn
    val timeStampColumn = readObj.timeStampColumn
    
    val indexDF = spark.read.parquet(wareHouseLocation + "/INDEX/" + tableName)
    indexDF.createOrReplaceTempView("indexDF_table")
    
    val Data_DF = spark.read.parquet(wareHouseLocation + "/DATA/" + tableName+"/*/*")
    Data_DF.createOrReplaceTempView("Data_DF_table")
    
    val joinCondition = primaryKeyColumn.split(",").map(a => "IN."+a+" = DA."+a).:+("IN.`"+timeStampColumn+"` = DA.`"+timeStampColumn+"`").mkString(" AND ")
    
    val ResOutput = spark.sql("SELECT * FROM Data_DF_table AS DA INNER JOIN indexDF_table AS IN ON "+joinCondition).drop(timeStampColumn)
    ResOutput.createOrReplaceTempView(outputTableName)
  }
}