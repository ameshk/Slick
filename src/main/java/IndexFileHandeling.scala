import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import java.util.Date
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path
import scala.collection.immutable.HashMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

class IndexFileHandeling(spark: SparkSession, hdfs: FileSystem) {

  def processFlow(in: IndexEncapsObj, mode: String): Boolean = {

    val inputTableName = in.inputTableName
    val delTableName = in.delTableName
    val indexTableName = in.indexTableName
    val newIndexTableName = in.newIndexTableName
    val delIndexTableName = in.delIndexTableName
    val primaryKeyColumn = in.primaryKeyColumn
    val timeStampColumn = in.timeStampColumn
    val indexInputFolderLocation = in.indexInputFolderLocation
    val indexOutputFolderLocation = indexInputFolderLocation + "_" + new Date().getTime
    val dateFolder = in.dateFolder
    val dateFolderColumn = in.partitionDateFolderColumn

    val flag = createCurrentIndexFile(indexTableName, indexInputFolderLocation)

    if(!flag)
    {
      createEmptyIndexFile(indexTableName, primaryKeyColumn, timeStampColumn,dateFolderColumn)
    }
    
    if (mode.toLowerCase().equals("update")) {
      createIndexData(newIndexTableName, inputTableName, primaryKeyColumn, timeStampColumn,dateFolderColumn,dateFolder)
      updateInsertRowsIndexFile("UPDATE_INDEX_TABLE", indexTableName, newIndexTableName, primaryKeyColumn, timeStampColumn,dateFolderColumn)
      writeIndexFile("UPDATE_INDEX_TABLE", primaryKeyColumn, timeStampColumn, indexInputFolderLocation, indexOutputFolderLocation)
    } else if (mode.toLowerCase().equals("delete")) {
      createIndexData(delIndexTableName, delTableName, primaryKeyColumn, timeStampColumn,dateFolderColumn,dateFolder)
      deleteRowsIndexFile("DELETE_INDEX_TABLE", indexTableName, delIndexTableName, primaryKeyColumn)
      writeIndexFile("DELETE_INDEX_TABLE", primaryKeyColumn, timeStampColumn, indexInputFolderLocation, indexOutputFolderLocation)
    }

    true
  }

  private def updateInsertRowsIndexFile(tableName: String, indexTableName: String, newIndexTableName: String, primaryKeyColumn: String, timeStampColumn: String,dateFolderColumn:String): Boolean =
    {
      /**
       * This method will update the index file
       */
      val currentIndexFileDataFrame = spark.sql("SELECT * FROM " + indexTableName)
      val newIndexFileDataFrame = spark.sql("SELECT * FROM " + newIndexTableName)

      val unionDF = currentIndexFileDataFrame.unionAll(newIndexFileDataFrame)
      unionDF.createOrReplaceTempView("unionIndexTable")

      val ResIndexFile = spark.sql("SELECT " + primaryKeyColumn + ", "+timeStampColumn+", "+dateFolderColumn+", DENSE_RANK() OVER (PARTITION BY "+primaryKeyColumn+","+dateFolderColumn+" ORDER BY "+timeStampColumn+" DESC) AS `ROW_DENSE_RANK` FROM unionIndexTable").filter("`ROW_DENSE_RANK` = '1'").drop("ROW_DENSE_RANK")

      ResIndexFile.createOrReplaceTempView(tableName)
      
      true
    }

  private def deleteRowsIndexFile(tableName: String, indexTableName: String, delIndexTableName: String, primaryKey: String): Boolean =
    {
      val joinCondition = primaryKey.map(a => "IN." + a + " = DEL." + a).mkString(" AND ")

      val currentIndexFileDataFrame = spark.sql("SELECT * FROM " + indexTableName)
      val deleteIndexDataFrame = spark.sql("SELECT * FROM " + indexTableName + " AS IN INNER JOIN " + delIndexTableName + " AS DEL ON " + joinCondition)

      val ResIndexFile = currentIndexFileDataFrame.except(deleteIndexDataFrame)

      ResIndexFile.createOrReplaceTempView(tableName)

      true
    }

  private def checkIndexData(tableName: String, primaryKeyColumn: String, timeStampColumn: String): Boolean =
    {
      /**
       * This will check if the primary key column is distinct
       */
      val df = spark.sql("SELECT COUNT(" + timeStampColumn + ") AS MAX_COUNT FROM " + tableName + " GROUP BY " + primaryKeyColumn)
      
      val countDF = df.filter("MAX_COUNT > 1").count()
      
      var flag = false
      if(countDF == 0)
      {
        flag = true
      }
      
      flag
    }

  private def createIndexData(tableName: String, inputTableName: String, primaryKeyColumn: String, timeStampColumn: String,dateFolderColumn:String, DateFolder:String): Boolean =
    {
      /**
       * This will create indexfile from input data
       */
      val df = spark.sql("SELECT " + primaryKeyColumn + " , " + timeStampColumn + ",'"+DateFolder+"' AS `"+dateFolderColumn+"` FROM " + inputTableName)
      df.createOrReplaceTempView(tableName)
      true
    }

  private def createCurrentIndexFile(tableName: String, indexInputFolderLocation: String): Boolean =
    {
      /**
       * This will create indexfile from input location
       */
      var flag = false
      try {
        val df = spark.read.parquet(indexInputFolderLocation)
        df.createOrReplaceTempView(tableName)
        flag = true
      } catch {
        case t: Throwable => {
          flag = false
        }
      }

      flag
    }
  
  private def createEmptyIndexFile(tableName: String, primaryKeyColumn: String, timeStampColumn:String, dateFolderColumn:String): Boolean =
    {
      /**
       * This will create indexfile from input location
       */
      var flag = false
      try {
        val col = primaryKeyColumn.split(",").map(a => StructField(a.replaceAll("`", ""),StringType,true)).:+(StructField(timeStampColumn.replaceAll("`", ""),StringType,true)).:+(StructField(dateFolderColumn.replaceAll("`", ""),StringType,true))
        val schema = StructType(col)
        val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
        df.createOrReplaceTempView(tableName)
        flag = true
      } catch {
        case t: Throwable => {
          flag = false
        }
      }

      flag
    }
  private def writeIndexFile(tableName: String, primaryKeyColumn: String, timeStampColumn: String, indexInputFolderLocation: String, indexOutputFolderLocation: String): Boolean =
    {
      /**
       * This method is to write index file
       */

      var flag = false
      if (checkIndexData(tableName, primaryKeyColumn, timeStampColumn)) {
        spark.sql("SELECT * FROM " + tableName).write.mode("overwrite").parquet(indexOutputFolderLocation)

        if (hdfs.isDirectory(new Path(indexInputFolderLocation))) {
          hdfs.delete(new Path(indexInputFolderLocation))
        }

        flag = hdfs.rename(new Path(indexOutputFolderLocation), new Path(indexInputFolderLocation))
      }
      flag
    }
}