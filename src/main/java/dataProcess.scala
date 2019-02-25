import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions._

class dataProcess(spark: SparkSession, hdfs: FileSystem) {
  val warehouseLocation = "file:///C:/Users/a.jayendra.karekar/Desktop/LiveWire/Files/DeltaData"
    
  def processInputData(tableName: String, primaryKey: String):Boolean= {
    val indexFolder = "/INDEX/" + tableName
    val DataFolder = "/DATA/" + tableName

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
  
  def readData(tableName:String , primaryKey:String, outputTableName:String)
  {
    
    val readObj = new ReadEncapsObj()
    
    readObj.primaryKeyColumn = primaryKey.split(",").map(a => "`" + a + "`").mkString(",")
    readObj.timeStampColumn = "EVENT_TIMESTAMP"
    
    val rd = new ReadData(spark, warehouseLocation)
    
    rd.readIndexData(tableName, readObj, outputTableName)
  }
}