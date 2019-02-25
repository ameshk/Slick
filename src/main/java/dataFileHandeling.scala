import java.util.Date
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path

class dataFileHandeling(spark: SparkSession, hdfs: FileSystem) extends Exception {
  
  private val dt = new Date()
  private val stf = new SimpleDateFormat("yyyyMMdd")
  private val formatDate = stf.format(dt)

  def upsertDataIngest(dn: dataEncapsObj, tableName: String): Boolean = {
    val primaryKeyColumn = dn.primaryKeyColumn
    val timeStampColumn = dn.timeStampColumn
    val indexTableName = dn.indexTableName
    val newIndexTableName = dn.newIndexTableName
    val indexInputFolderLocation = dn.indexInputFolderLocation
    val opDataFolderLocation = dn.opDataFolderLocation

    val CurrentTimestamp = new Date().getTime.toString()

    val tempIndexData = spark.sql("SELECT * ,'" + CurrentTimestamp + "' AS " + timeStampColumn + " FROM " + tableName)

    tempIndexData.createOrReplaceTempView("TempInputTable")

    val dupPK = spark.sql("SELECT " + primaryKeyColumn + ",COUNT(" + timeStampColumn + ") AS MAX_COUNT FROM TempInputTable GROUP BY " + primaryKeyColumn).filter("MAX_COUNT > 1").count

    if (dupPK > 0) {
      throw new Exception("Duplicates in Primary Key")
    }

    val upsertIndexObj = new IndexEncapsObj()

    upsertIndexObj.inputTableName = "TempInputTable"
    upsertIndexObj.delTableName = "NA"
    upsertIndexObj.indexTableName = "CURRENT_INDEX_TABLE"
    upsertIndexObj.newIndexTableName = newIndexTableName
    upsertIndexObj.delIndexTableName = "NA"
    upsertIndexObj.primaryKeyColumn = primaryKeyColumn
    upsertIndexObj.timeStampColumn = timeStampColumn
    upsertIndexObj.indexInputFolderLocation = indexInputFolderLocation
    upsertIndexObj.dateFolder = formatDate
    upsertIndexObj.partitionDateFolderColumn = "PARTITION_DATE_FOLDER"
      
    val upsertFlag = processIndexFile(upsertIndexObj, "update")

    if (upsertFlag) {
      writeOutputDataPartitioned("TempInputTable", opDataFolderLocation)
    } else {
      throw new Exception("Error while doing Upsert")
    }

    true
  }

  def deleteData(dn: dataEncapsObj, tableName: String): Boolean =
    {
      val primaryKeyColumn = dn.primaryKeyColumn
      val timeStampColumn = dn.timeStampColumn
      val indexTableName = dn.indexTableName
      val newIndexTableName = dn.newIndexTableName
      val indexInputFolderLocation = dn.indexInputFolderLocation
      val opDataFolderLocation = dn.opDataFolderLocation
      
      val upsertIndexObj = new IndexEncapsObj()

      upsertIndexObj.inputTableName = "NA"
      upsertIndexObj.delTableName = tableName
      upsertIndexObj.indexTableName = "CURRENT_INDEX_TABLE"
      upsertIndexObj.newIndexTableName = newIndexTableName
      upsertIndexObj.delIndexTableName = "DELETE_INDEX_TABLE"
      upsertIndexObj.primaryKeyColumn = primaryKeyColumn
      upsertIndexObj.timeStampColumn = timeStampColumn
      upsertIndexObj.indexInputFolderLocation = indexInputFolderLocation
      upsertIndexObj.dateFolder = formatDate
      upsertIndexObj.partitionDateFolderColumn = "PARTITION_DATE_FOLDER"

      processIndexFile(upsertIndexObj, "delete")
      
    }

  def insertDataIngest(dn: dataEncapsObj, tableName: String): Boolean =
    {
      val opDataFolderLocation = dn.opDataFolderLocation
      writeOutputDataPartitioned(tableName, opDataFolderLocation)
    }

  private def processIndexFile(indexEnObj: IndexEncapsObj, mode: String): Boolean =
    {
      val indFileHand = new IndexFileHandeling(spark, hdfs)

      indFileHand.processFlow(indexEnObj, mode)
    }

  private def writeOutputDataPartitioned(tableName: String, opDataFolderLocation: String): Boolean =
    {
      
      val ResOutputPath = opDataFolderLocation+"/"+formatDate
      
      if(!hdfs.isDirectory(new Path(ResOutputPath)))
      {
        hdfs.mkdirs(new Path(ResOutputPath))
      }

      spark.sql("SELECT * FROM " + tableName).write.mode("append").parquet(ResOutputPath)
      true
    }
}