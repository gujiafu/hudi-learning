package cn.itcast.hudi.stream

import cn.itcast.hudi.didi.SparkUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * 基于StructuredStreaming结构化流实时从Kafka消费数据，经过ETL转换后，存储至Hudi表
 */
object HudiStructuredDemo {
	
	/**
	 * 指定Kafka Topic名称，实时消费数据
	 */
	def readFromKafka(spark: SparkSession, topicName: String): DataFrame = {
		spark.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", "node1.itcast.cn:9092")
			.option("subscribe", topicName)
			.option("startingOffsets", "latest")
			.option("maxOffsetsPerTrigger", 100000)
			.option("failOnDataLoss", "false")
    		.load()
	}
	
	/**
	 * 对Kafka获取数据，进行转换操作，获取所有字段的值，转换为String，以便保存Hudi表中
	 */
	def process(streamDF: DataFrame): DataFrame = {
		streamDF
			// 选择字段
    		.selectExpr(
			    "CAST(key AS STRING) order_id",
			    "CAST(value AS STRING) AS message",
			    "topic", "partition", "offset", "timestamp"
		    )
			// 解析Message数据，提取字段值
    		.withColumn("user_id", get_json_object(col("message"), "$.userId"))
    		.withColumn("order_time", get_json_object(col("message"), "$.orderTime"))
    		.withColumn("ip", get_json_object(col("message"), "$.ip"))
    		.withColumn("order_money", get_json_object(col("message"), "$.orderMoney"))
    		.withColumn("order_status", get_json_object(col("message"), "$.orderStatus"))
			// 删除message字段
    		.drop(col("message"))
			// 转换订单日期时间格式为Long类型，作为hudi表中合并数据字段
    		.withColumn("ts", to_timestamp(col("order_time"), "yyyy-MM-dd HH:mm:ss.SSS"))
			// 订单日期时间提取分区日志：yyyyMMdd
    		.withColumn("day", substring(col("order_time"), 0, 10))
	}
	
	/**
	 * 将流式数据DataFrame保存到Hudi表中
	 */
	def saveToHudi(streamDF: DataFrame): Unit = {
		streamDF.writeStream
    		.outputMode(OutputMode.Append())
    		.queryName("query-hudi-streaming")
    		.foreachBatch((batchDF: Dataset[Row], batchId: Long) => {
			    println(s"============== BatchId: ${batchId} start ==============")
			    import org.apache.hudi.DataSourceWriteOptions._
			    import org.apache.hudi.config.HoodieWriteConfig._
			    import org.apache.hudi.keygen.constant.KeyGeneratorOptions._
			    
			    batchDF.write
				    .mode(SaveMode.Append)
				    .format("hudi")
				    .option("hoodie.insert.shuffle.parallelism", "2")
				    .option("hoodie.upsert.shuffle.parallelism", "2")
				    // Hudi 表的属性值设置
				    .option(RECORDKEY_FIELD.key(), "order_id")
				    .option(PRECOMBINE_FIELD.key(), "ts")
				    .option(PARTITIONPATH_FIELD.key(), "day")
				    .option(TBL_NAME.key(), "tbl_hudi_order")
    			    .option(TABLE_TYPE.key(), "MERGE_ON_READ")
				    // 分区值对应目录格式，与Hive分区策略一致
				    .option(HIVE_STYLE_PARTITIONING_ENABLE.key(), "true")
				    .save("/hudi-warehouse/tbl_hudi_order")
		    })
    		.option("checkpointLocation", "/datas/hudi-spark/struct-ckpt-1001")
    		.start()
	}
	
	def main(args: Array[String]): Unit = {
		// step1、构建SparkSession实例对象
		val spark: SparkSession = SparkUtils.createSpakSession(this.getClass)
		
		//step2、从Kafka实时消费数据
		val kafkaStreamDF: DataFrame = readFromKafka(spark, "order-topic")
		
		// step3、提取数据，转换数据类型
		val streamDF: DataFrame = process(kafkaStreamDF)
		
		// step4、保存数据至Hudi表中：MOR类型表，读取表数据合并文件
		saveToHudi(streamDF)
		
		// step5、流式应用启动以后，等待终止
		spark.streams.active.foreach(query => println(s"Query: ${query.name} is Running .........."))
		spark.streams.awaitAnyTermination()
	}
	
}
