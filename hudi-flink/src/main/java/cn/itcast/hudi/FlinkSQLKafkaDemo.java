package cn.itcast.hudi;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.* ;

public class FlinkSQLKafkaDemo {

	public static void main(String[] args) {
		// 1-获取表执行环境
		EnvironmentSettings settings = EnvironmentSettings
			.newInstance()
			.inStreamingMode() // 设置流式模式
			.build();
		TableEnvironment tableEnv = TableEnvironment.create(settings);

		// 2-创建输入表，TODO：从Kafka消费数据
		tableEnv.executeSql(
			"CREATE TABLE order_kafka_source (\n" +
				"  orderId STRING,\n" +
				"  userId STRING,\n" +
				"  orderTime STRING,\n" +
				"  ip STRING,\n" +
				"  orderMoney DOUBLE,\n" +
				"  orderStatus INT\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'order-topic',\n" +
				"  'properties.bootstrap.servers' = 'node1.itcast.cn:9092',\n" +
				"  'properties.group.id' = 'gid-1001',\n" +
				"  'scan.startup.mode' = 'latest-offset',\n" +
				"  'format' = 'json',\n" +
				"  'json.fail-on-missing-field' = 'false',\n" +
				"  'json.ignore-parse-errors' = 'true'\n" +
				")"
		);

		// 3-转换数据：可以使用SQL，也可以时Table API
		Table etlTable = tableEnv
			.from("order_kafka_source")
			// 添加字段：Hudi表分区字段， "orderTime": "2021-11-22 10:34:34.136" -> 021-11-22
			.addColumns(
				$("orderTime").substring(0, 10).as("partition_day")
			)
			// 添加字段：Hudi表数据合并字段，时间戳, "orderId": "20211122103434136000001" ->  20211122103434136
			.addColumns(
				$("orderId").substring(0, 17).as("ts")
			);
		tableEnv.createTemporaryView("view_order", etlTable);

		// 4-创建输入表，TODO: 将结果数据进行输出
		tableEnv.executeSql("SELECT * FROM view_order").print();
	}

}
