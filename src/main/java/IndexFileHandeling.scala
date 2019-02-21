import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import java.util.Date
import org.apache.hadoop.fs.Path
import scala.collection.immutable.HashMap

class IndexFileHandeling(configMap: HashMap[String, String], spark: SparkSession, hdfs: FileSystem) {

  def processFlow(in: IndexEncapsObj, mode: String) {

    val inputTableName = in.inputTableName
    val delTableName = in.delTableName
    val indexTableName = in.indexTableName
    val newIndexTableName = in.newIndexTableName
    val delIndexTableName = in.delIndexTableName
    val primaryKeyColumn = in.primaryKeyColumn
    val timeStampColumn = in.timeStampColumn
    val indexInputFolderLocation = in.indexInputFolderLocation
    val indexOutputFolderLocation = indexInputFolderLocation + "_" + new Date().getTime

    createCurrentIndexFile(indexTableName, indexInputFolderLocation)

    if (mode.toLowerCase().equals("update")) {
      createIndexData(newIndexTableName, inputTableName, primaryKeyColumn, timeStampColumn)
      updateInsertRowsIndexFile("UPDATE_INDEX_TABLE", indexTableName, newIndexTableName, primaryKeyColumn, timeStampColumn)
      writeIndexFile("UPDATE_INDEX_TABLE", primaryKeyColumn, timeStampColumn, indexInputFolderLocation, indexOutputFolderLocation)
    } else if (mode.toLowerCase().equals("delete")) {
      createIndexData(delIndexTableName, delTableName, primaryKeyColumn, timeStampColumn)
      deleteRowsIndexFile("DELETE_INDEX_TABLE", indexTableName, delIndexTableName)
      writeIndexFile("DELETE_INDEX_TABLE", primaryKeyColumn, timeStampColumn, indexInputFolderLocation, indexOutputFolderLocation)
    }
  }

  private def updateInsertRowsIndexFile(tableName: String, indexTableName: String, newIndexTableName: String, primaryKeyColumn: String, timeStampColumn: String): Boolean =
    {
      /**
       * This method will update the index file
       */
      val currentIndexFileDataFrame = spark.sql("SELECT * FROM " + indexTableName)
      val newIndexFileDataFrame = spark.sql("SELECT * FROM " + newIndexTableName)

      val unionDF = currentIndexFileDataFrame.unionAll(newIndexFileDataFrame)
      unionDF.createOrReplaceTempView("unionIndexTable")

      val ResIndexFile = spark.sql("SELECT " + primaryKeyColumn + ", MAX(" + timeStampColumn + ") AS " + timeStampColumn + " FROM unionIndexTable GROUP BY " + primaryKeyColumn)

      ResIndexFile.createOrReplaceTempView(tableName)

      true
    }

  private def deleteRowsIndexFile(tableName: String, indexTableName: String, delIndexTableName: String): Boolean =
    {
      val currentIndexFileDataFrame = spark.sql("SELECT * FROM " + indexTableName)
      val deleteIndexDataFrame = spark.sql("SELECT * FROM " + delIndexTableName)

      val ResIndexFile = currentIndexFileDataFrame.except(deleteIndexDataFrame)

      ResIndexFile.createOrReplaceTempView(tableName)

      true
    }

  private def checkIndexData(tableName: String, primaryKeyColumn: String, timeStampColumn: String): Boolean =
    {
      /**
       * This will check if the primary key column is distinct
       */
      spark.sql("SELECT *,COUNT(" + timeStampColumn + ") AS MAX_COUNT FROM " + tableName + " GROUP BY " + primaryKeyColumn).filter("MAX_COUNT > 1").count() == 0
    }

  private def createIndexData(tableName: String, inputTableName: String, primaryKeyColumn: String, timeStampColumn: String): Boolean =
    {
      /**
       * This will create indexfile from input data
       */
      val df = spark.sql("SELECT " + primaryKeyColumn + " , " + timeStampColumn + " FROM " + inputTableName)
      df.createOrReplaceTempView(tableName)
      true
    }

  private def createCurrentIndexFile(tableName: String, indexInputFolderLocation: String): Boolean =
    {
      /**
       * This will create indexfile from input location
       */
      val df = spark.read.parquet(indexInputFolderLocation)
      df.createOrReplaceTempView(tableName)
      true
    }
  private def writeIndexFile(tableName: String, primaryKeyColumn: String, timeStampColumn: String, indexInputFolderLocation: String, indexOutputFolderLocation: String): Boolean =
    {
      /**
       * This method is to write index file
       */

      var flag = false
      if (checkIndexData(tableName, primaryKeyColumn, timeStampColumn)) {
        spark.sql("SELECT * FROM " + tableName).write.mode("overwrite").parquet(indexOutputFolderLocation)

        hdfs.delete(new Path(indexInputFolderLocation))

        flag = hdfs.rename(new Path(indexOutputFolderLocation), new Path(indexInputFolderLocation))
      }
      flag
    }
}