package cn.itcast.hudi.didi

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 滴滴海口出行运营数据分析，使用SparkSQL操作数据，先读取CSv文件，保存至Hudi表中。
 * step1. 构建SparkSession实例对象（集成Hudi和HDFS）
 * step2. 加载本地CSV文件格式滴滴出行数据
 * step3. 滴滴出行数据ETL处理
 * stpe4. 保存转换后数据至Hudi表
 * step5. 应用结束关闭资源
 */
object DidiStorageSpark {
	
	// 滴滴数据路径
	val datasPath: String = "file:///D:/hudi-learning/datas/didi/dwv_order_make_haikou_1.txt"
	
	// Hudi 中表的属性
	val hudiTableName: String = "tbl_didi_haikou"
	val hudiTablePath: String = "/hudi-warehouse/tbl_didi_haikou"
	
	/**
	 * 读取CSV格式文本文件数据，封装到DataFrame中
	 */
	def readCsvFile(spark: SparkSession, path: String): DataFrame = {
		spark.read
			// 设置分隔符为制表符
			.option("sep", "\\t")
    		// 文件首行为列名称
    		.option("header", "true")
			// 依据数值自动推断数据类型
    		.option("inferSchema", "true")
			// 指定文件路径
			.csv(path)
	}
	
	/**
	 * 对滴滴出行海口数据进行ETL转换操作：指定ts和partitionpath列
	 */
	def process(dataframe: DataFrame): DataFrame = {
		dataframe
			// 添加字段，就是Hudi表分区字段，三级分区 -> yyyy-MM-dd
    		.withColumn(
			    "partitionpath",
			    concat_ws("-", col("year"), col("month"), col("day"))
		    )
			// 删除列
    		.drop("year", "month", "day")
			// 添加timestamp列，作为Hudi表记录数据合并时字段，使用发车时间
    		.withColumn(
			    "ts",
			    unix_timestamp(col("departure_time"), "yyyy-MM-dd HH:mm:ss")
		    )
	}
	
	/**
	 * 将数据集DataFrame保存至Hudi表中，表的类型为COW，属于批量保存数据，写少读多
	 */
	def saveToHudi(dataframe: DataFrame, table: String, path: String): Unit = {
		// 导入包
		import org.apache.hudi.DataSourceWriteOptions._
		import org.apache.hudi.config.HoodieWriteConfig._
		
		// 保存数据
		dataframe.write
    		.mode(SaveMode.Overwrite)
    		.format("hudi")
			.option("hoodie.insert.shuffle.parallelism", "2")
			.option("hoodie.upsert.shuffle.parallelism", "2")
			// Hudi 表的属性值设置
			.option(RECORDKEY_FIELD.key(), "order_id")
			.option(PRECOMBINE_FIELD.key(), "ts")
			.option(PARTITIONPATH_FIELD.key(), "partitionpath")
			.option(TBL_NAME.key(), table)
    		.save(path)
	}
	
	def main(args: Array[String]): Unit = {
		// step1. 构建SparkSession实例对象（集成Hudi和HDFS
		val spark: SparkSession = SparkUtils.createSpakSession(this.getClass)
		
		// step2. 加载本地CSV文件格式滴滴出行数据
		val didiDF = readCsvFile(spark, datasPath)
//		didiDF.printSchema()
//		didiDF.show(10, truncate = false)
		
		// step3. 滴滴出行数据ETL处理
		val etlDF: DataFrame = process(didiDF)
//		etlDF.printSchema()
//		etlDF.show(10, truncate = false)
		
		// stpe4. 保存转换后数据至Hudi表
		saveToHudi(etlDF, hudiTableName, hudiTablePath)
		
		// step5. 应用结束关闭资源
		spark.stop()
	}
	
}
