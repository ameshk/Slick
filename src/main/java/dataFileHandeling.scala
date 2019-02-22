import java.util.Date
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import java.text.SimpleDateFormat

class dataFileHandeling(spark: SparkSession, hdfs: FileSystem) extends Exception {

  def upsertDataIngest(dn: dataEncapsObj, tableName: String): Boolean = {
    val primaryKeyColumn = dn.primaryKeyColumn
    val timeStampColumn = dn.timeStampColumn
    val indexTableName = dn.indexTableName
    val newIndexTableName = dn.newIndexTableName
    val indexInputFolderLocation = dn.indexInputFolderLocation
    val opDataFolderLocation = dn.opDataFolderLocation

    val CurrentTimestamp = new Date().getTime.toString()

    val tempIndexData = spark.sql("SELECT * ,'" + CurrentTimestamp + "' AS " + timeStampColumn + " FROM " + tableName)

    tempIndexData.createOrReplaceTempView("TempIndexTable")

    val dupPK = spark.sql("SELECT " + primaryKeyColumn + ",COUNT(" + timeStampColumn + ") AS MAX_COUNT FROM TempIndexTable GROUP BY " + primaryKeyColumn).filter("MAX_COUNT > 1").count

    if (dupPK > 0) {
      throw new Exception("Duplicates in Primary Key")
    }

    val upsertIndexObj = new IndexEncapsObj()

    upsertIndexObj.inputTableName = tableName
    upsertIndexObj.delTableName = "NA"
    upsertIndexObj.indexTableName = "CURRENT_INDEX_TABLE"
    upsertIndexObj.newIndexTableName = newIndexTableName
    upsertIndexObj.delIndexTableName = "NA"
    upsertIndexObj.primaryKeyColumn = primaryKeyColumn
    upsertIndexObj.timeStampColumn = timeStampColumn
    upsertIndexObj.indexInputFolderLocation = indexInputFolderLocation

    val upsertFlag = processIndexFile(upsertIndexObj, "update")

    if (upsertFlag) {
      writeOutputDataPartitioned("TempIndexTable", opDataFolderLocation)
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
      val dt = new Date()
      val stf = new SimpleDateFormat("yyyyMMdd")
      val formatDate = stf.format(dt)

      spark.sql("SELECT *,'" + formatDate + "' AS PARTITIONED_DATE FROM " + tableName).write.partitionBy("PARTITIONED_DATE").mode("append").parquet(opDataFolderLocation)
      true
    }
}