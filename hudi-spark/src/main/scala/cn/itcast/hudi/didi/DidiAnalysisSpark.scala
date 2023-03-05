package cn.itcast.hudi.didi

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

/**
 * 滴滴海口出行运营数据分析，使用SparkSQL操作数据，加载Hudi表数据，按照业务需求统计。
 */
object DidiAnalysisSpark {
	
	// Hudi表属性，存储数据HDFS路径
	val hudiTablePath: String = "/hudi-warehouse/tbl_didi_haikou"
	
	/**
	 * 加载Hudi表数据，封装到DataFrame中
	 */
	def readFromHudi(spark: SparkSession, path: String): DataFrame = {
		val didiDF: DataFrame = spark.read.format("hudi").load(path)
		
		// 选择字段
		didiDF.select(
			"product_id", "type", "traffic_type", "pre_total_fee", "start_dest_distance", "departure_time"
		)
	}
	
	/**
	 * 订单类型统计，字段：product_id
	 */
	def reportProduct(dataframe: DataFrame): Unit = {
		// a. 按照产品线ID分组统计即可
		val reportDF: DataFrame = dataframe.groupBy("product_id").count()
		
		// b. 自定义UDF函数，转换名称
		val to_name = udf(
			(productId: Int) => {
				productId match {
					case 1 => "滴滴专车"
					case 2 => "滴滴企业专车"
					case 3 => "滴滴快车"
					case 4 => "滴滴企业快车"
				}
			}
		)
		
		// c. 转换名称
		val resultDF: DataFrame = reportDF.select(
			to_name(col("product_id")).as("order_type"),
			col("count").as("total")
		)
		resultDF.printSchema()
		resultDF.show(10, truncate = false)
	}
	
	
	/**
	 * 订单时效性统计，字段：type
	 */
	def reportType(dataframe: DataFrame): Unit = {
		// a. 按照时效性id分组统计即可
		val reportDF: DataFrame = dataframe.groupBy("type").count()
		
		// b. 自定义UDF函数，转换名称
		val to_name = udf(
			(realtimeType: Int) => {
				realtimeType match {
					case 0 => "实时"
					case 1 => "预约"
				}
			}
		)
		
		// c. 转换名称
		val resultDF: DataFrame = reportDF.select(
			to_name(col("type")).as("order_realtime"),
			col("count").as("total")
		)
		resultDF.printSchema()
		resultDF.show(10, truncate = false)
	}
	
	/**
	 * 交通类型统计，字段：traffic_type
	 */
	def reportTraffic(dataframe: DataFrame): Unit = {
		// a. 按照交通类型id分组统计即可
		val reportDF: DataFrame = dataframe.groupBy("traffic_type").count()
		
		// b. 自定义UDF函数，转换名称
		val to_name = udf(
			(trafficType: Int) => {
				trafficType match {
					case 0 =>  "普通散客"
					case 1 =>  "企业时租"
					case 2 =>  "企业接机套餐"
					case 3 =>  "企业送机套餐"
					case 4 =>  "拼车"
					case 5 =>  "接机"
					case 6 =>  "送机"
					case 302 =>  "跨城拼车"
					case _ => "未知"
				}
			}
		)
		
		// c. 转换名称
		val resultDF: DataFrame = reportDF.select(
			to_name(col("traffic_type")).as("traffic_type"),
			col("count").as("total")
		)
		resultDF.printSchema()
		resultDF.show(10, truncate = false)
	}
	
	/**
	 * 订单价格统计，先将订单价格划分阶段，再统计各个阶段数目，使用字段：pre_total_fee
	 */
	def reportPrice(dataframe: DataFrame): Unit = {
		val resultDF: DataFrame = dataframe
    		.agg(
			    // 价格 0 ~ 15
			    sum(
				    when(col("pre_total_fee").between(0, 15), 1).otherwise(0)
			    ).as("0~15"),
			    // 价格 16 ~ 30
			    sum(
				    when(col("pre_total_fee").between(16, 30), 1).otherwise(0)
			    ).as("16~30"),
			    // 价格 31 ~ 50
			    sum(
				    when(col("pre_total_fee").between(31, 50), 1).otherwise(0)
			    ).as("31~50"),
			    // 价格 51 ~ 100
			    sum(
				    when(col("pre_total_fee").between(51, 100), 1).otherwise(0)
			    ).as("51~100"),
			    // 价格 100+
			    sum(
				    when(col("pre_total_fee").gt(100), 1).otherwise(0)
			    ).as("100+")
		    )
		resultDF.printSchema()
		resultDF.show(10, truncate = false)
	}
	
	
	/**
	 * 订单距离统计，先将订单距离划分为不同区间，再统计各个区间数目，使用字段：start_dest_distance
	 */
	def reportDistance(dataframe: DataFrame): Unit = {
		val resultDF: DataFrame = dataframe
			.agg(
				// 距离： 0 ~ 10km
				sum(
					when(col("start_dest_distance").between(0, 10000), 1).otherwise(0)
				).as("0~10km"),
				// 距离： 10 ~ 20km
				sum(
					when(col("start_dest_distance").between(10001, 20000), 1).otherwise(0)
				).as("10~20km"),
				// 距离： 20 ~ 20km
				sum(
					when(col("start_dest_distance").between(20001, 30000), 1).otherwise(0)
				).as("20~30"),
				// 距离： 30 ~ 50km
				sum(
					when(col("start_dest_distance").between(30001, 50000), 1).otherwise(0)
				).as("30~50km"),
				// 距离： 50km+
				sum(
					when(col("start_dest_distance").gt(50001), 1).otherwise(0)
				).as("50+km")
			)
		resultDF.printSchema()
		resultDF.show(10, truncate = false)
	}
	
	/**
	 * 订单星期分组统计，先将日期转换为星期，再对星期分组统计，使用字段：departure_time
	 */
	def reportWeek(dataframe: DataFrame): Unit = {
		// a. 自定义UDF函数，转换日期为星期
		val to_week: UserDefinedFunction = udf(
			(dateStr: String) => {
				val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
				val calendar: Calendar = Calendar.getInstance();
				
				val date: Date = format.parse(dateStr)
				calendar.setTime(date)
				
				val dayWeek = calendar.get(Calendar.DAY_OF_WEEK) match {
					case 1 => "星期日"
					case 2 => "星期一"
					case 3 => "星期二"
					case 4 => "星期三"
					case 5 => "星期四"
					case 6 => "星期五"
					case 7 => "星期六"
				}
				// 返回星期即可
				dayWeek
			}
		)
		
		// b. 对数据处理，使用udf函数
		val reportDF: DataFrame = dataframe
    		.select(
			    to_week(col("departure_time")).as("week")
		    )
    		.groupBy("week").count()
    		.select(
			    col("week"), col("count").as("total")
		    )
		reportDF.printSchema()
		reportDF.show(10, truncate = false)
	}
	
	def main(args: Array[String]): Unit = {
		// step1、构建SparkSession实例对象（集成Hudi和HDFS）
		val spark: SparkSession = SparkUtils.createSpakSession(this.getClass, partitions = 8)
	
		// step2、加载Hudi表的数据，指定字段
		val hudiDF: DataFrame = readFromHudi(spark, hudiTablePath)
//		hudiDF.printSchema()
//		hudiDF.show(10, truncate = false)
		
		// 由于数据被使用多次，所以建议缓存
		hudiDF.persist(StorageLevel.MEMORY_AND_DISK)
		
		// step3、按照业务指标进行统计分析
		// 指标1：订单类型统计
//		reportProduct(hudiDF)
		
		// 指标2：订单时效统计
//		reportType(hudiDF)
		
		// 指标3：交通类型统计
//		reportTraffic(hudiDF)
		
		// 指标4：订单价格统计
//		reportPrice(hudiDF)
		
		// 指标5：订单距离统计
//		reportDistance(hudiDF)
		
		// 指标6：日期类型 -> 星期，进行统计
		reportWeek(hudiDF)
		
		// 当数据不在使用时，释放资源
		hudiDF.unpersist()
		
		// step4、应用结束，关闭资源
		spark.stop()
	}
	
}
