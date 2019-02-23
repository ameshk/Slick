import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.Path


object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TEST_UPSERT").master("local[*]").getOrCreate()
    
    /*val DF = spark.read.format("com.databricks.spark.csv").option("header","true").load("file:///C:/Users/a.jayendra.karekar/Desktop/LiveWire/Files/Documents/emp.csv")
    DF.createOrReplaceTempView("EMPLOYEE_TABLE")
    */
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("file:///C:/Users/a.jayendra.karekar/Desktop"), hadoopConf)
    
    //spark.sql("SELECT MAX(EMP_ID) AS EMPID FROM EMPLOYEE_TABLE").show
   
    /*val dp = new dataProcess(spark,hdfs)
    
    dp.processInputData("EMPLOYEE_TABLE", "EMP_ID")*/
    
    val DF = spark.read.parquet("file:///C:/Users/a.jayendra.karekar/Desktop/LiveWire/Files/DeltaData/DATA/EMPLOYEE_TABLE")
    
    DF.show
    
     val DF2 = spark.read.parquet("file:///C:/Users/a.jayendra.karekar/Desktop/LiveWire/Files/DeltaData/INDEX/EMPLOYEE_TABLE")
     
     DF2.show
  }
}