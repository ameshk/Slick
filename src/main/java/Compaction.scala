import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import java.util.Date


class Compaction(WarehouseLocation:String,hdfs:FileSystem, spark:SparkSession) {
  
  def majorCompaction(tableName:String,CompEncObj:CompactionEncapsObj)
  {
    
    val primaryKeyColumn = CompEncObj.primaryKeyColumn
    val timeStampColumn = CompEncObj.timeStampColumn
    val partitionDateFolderColumn = CompEncObj.partitionDateFolderColumn
    
    val IndexFilePath = WarehouseLocation+"/INDEX/"+tableName
    val DataFilePath = WarehouseLocation+"/DATA/"+tableName
    
    val IndexDF = spark.read.parquet(IndexFilePath)
      
    val FolderList = hdfs.globStatus(new Path((DataFilePath+"/*"))).map(a => a.getPath.toString())
    
    for(Folderpath <- FolderList)
    {
      val DataDF = spark.read.parquet(Folderpath)
      DataDF.createOrReplaceTempView("DataDF_Table")
      
      val currentTimestamp = new Date().getTime.toString
      
      val DateFolder = Folderpath.split("/").last
      
      val newOutPutPath = DataFilePath+"/"+DateFolder+"_"+currentTimestamp
      
      val FilteredIndexDF = IndexDF.filter("`"+partitionDateFolderColumn+"` = '"+DateFolder+"'")
      FilteredIndexDF.createOrReplaceTempView("FilteredIndex_Table")
      
      val joinCondition = primaryKeyColumn.split(",").map(a => "DA."+a+" = FT."+a).:+("DA.`"+timeStampColumn+"` = FT.`"+timeStampColumn+"`").mkString(" AND ")
      
      val CompactedDF = spark.sql("SELECT DA.* FROM DataDF_Table AS DA INNER JOIN FilteredIndex_Table AS FT ON "+joinCondition)
      
      CompactedDF.write.mode("overwrite").parquet(newOutPutPath)
      
      hdfs.delete(new Path(Folderpath))
      
      hdfs.rename(new Path(newOutPutPath), new Path(Folderpath))
    }
  }
}