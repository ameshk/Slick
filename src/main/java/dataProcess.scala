import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions._

class dataProcess(spark: SparkSession, hdfs: FileSystem) {
  def processInputData(tableName: String, primaryKey: String):Boolean= {
    val indexFolder = "file:///C:/Users/a.jayendra.karekar/Desktop/LiveWire/Files/DeltaData/INDEX/" + tableName
    val DataFolder = "file:///C:/Users/a.jayendra.karekar/Desktop/LiveWire/Files/DeltaData/DATA/" + tableName

    val dn = new dataEncapsObj()
    dn.primaryKeyColumn = primaryKey.split(",").map(a => "`" + a + "`").mkString(",")
    dn.timeStampColumn = "EVENT_TIMESTAMP"
    dn.indexTableName = ""
    dn.newIndexTableName = "NEW_INDEX_TABLE"
    dn.indexInputFolderLocation = indexFolder
    dn.opDataFolderLocation = DataFolder
    
    val dfh = new dataFileHandeling(spark,hdfs)
    
    dfh.upsertDataIngest(dn, tableName)
    true
  }
}