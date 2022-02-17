package tr.com.saglik.spark.history.processor;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class CheckStreamProcessor {
	private static final Logger logger = Logger.getLogger(HesCodeCheckStreamProcessor.class);
	
	public static void main(String[] args) throws Exception {
		logger.error("Start Kafka Stream");

		//read Spark and Cassandra properties and create SparkConf
		Properties props = PropertyFileReader.getInstance().getProperties();

		if (props.isEmpty()) {
			logger.error("properties file not found");
			throw new RuntimeException("properties file not found");
		} else {
			props.forEach((k, v) -> logger.error(k + " : " + v));
		}
		
		SparkSession session = SparkSession
				.builder()
				.appName(props.getProperty("spark.app.name"))
				//.config("spark.master", props.getProperty("spark.master"))
				.config("spark.clickhouse.connection.per.executor.max", "2")
				.config("spark.clickhouse.metrics.enable", "false")
				.config("spark.clickhouse.socket.timeout.ms", "10000")
				.config("spark.clickhouse.cluster.auto-discovery", "true")
//				.config("spark.driver.extraClassPath","./clickhouse-native-jdbc-shaded-2.5.4.jar")
//				.config("spark.clickhouse.driver","ru.yandex.clickhouse.ClickHouseDriver")
				//.config("cassandra.output.throughput_mb_per_sec", 1)
		        //.config("spark.cassandra.output.consistency.level", "ONE")			
		        //.config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT") 
		        //.config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT") 
		        .config("spark.sql.session.timeZone", "UTC")
				.getOrCreate();

		session.sparkContext().setLogLevel("ERROR");

		StructType schemaStream = new StructType()
				.add("payload", DataTypes.StringType);
		
		StructType schemaTs = new StructType()
				.add("$numberLong", DataTypes.StringType);
		StructType schemaId = new StructType()
				.add("$oid", DataTypes.StringType);

		StructType schemaPayload = new StructType()
				.add("timestamp",  schemaTs)
				.add("_id", schemaId)
				.add("service_provider", DataTypes.StringType)
				.add("inspector_identity_hash", DataTypes.StringType)
				.add("is_blocked", DataTypes.BooleanType)
				.add("identity_hash", DataTypes.StringType);

		// Subscribe to 1 topic
		Dataset<Row> df = session
				.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", props.getProperty("kafka.brokerlist"))
				.option("subscribe", props.getProperty("kafka.topic"))
				.option("startingOffsets", "earliest")
				.option("group.id", props.getProperty("kafka.groupId"))
				.option("kafka.session.timeout.ms", "30000")
				//.option("fetch.max.wait.ms", "20500")
				//.option("heartbeat.interval.ms", "6000")
				.option("enable.auto.commit", "false")
				.option("maxOffsetsPerTrigger", Long.valueOf(props.getProperty("spark.maxOffsetsPerTrigger")))
				//.option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
				.load()
				.select(from_json(col("value").cast("string"), schemaStream)
						.alias("parsed_value"))
				.select(from_json(col("parsed_value.payload").cast("string"), schemaPayload)
						.alias("payload"),
						col("payload.timestamp.$numberLong").cast("long").alias("epoch_timestamp"),
						concat(from_unixtime(col("payload.timestamp.$numberLong").cast("double").divide((double)1000.0), "yyyy-MM-dd'T'HH:mm:ss"),
								lit("."),
								substring(col("payload.timestamp.$numberLong").cast("string"), 11, 3),
								lit("Z")).as("check_timestamp"),
						col("payload.service_provider").as("service_provider"),
						when(coalesce(col("payload.inspector_identity_hash"), lit("UNKNOWN")).eqNullSafe("UNKNOWN"), col("payload.service_provider").cast("string")).otherwise(col("payload.inspector_identity_hash").cast("string")).as("checked_by_user"),
						col("payload.is_blocked").as("is_blocked"),
						col("payload.identity_hash").as("identity_hash"),
						col("payload.health_status").as("health_status"),
						col("payload._id.$oid").cast("string").as("ref_id"));
		
		df.createOrReplaceTempView("code_check");


		StreamingQuery query = df.writeStream()
				.foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
					@Override
					public void call(Dataset<Row> writeDF, Long batchId) throws Exception {
						Dataset<Row> byUserDF = writeDF
								.select(col("checked_by_user"), col("check_timestamp"), col("health_status"),
										to_date(col("check_timestamp")).as("checked_date"));
						

						byUserDF
						    .write()
							.format("jdbc")
							.option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
					        .option("url", "jdbc:clickhouse://xxxxx)
							.option("user","kds_user")
							.option("max_insert_block_size","1024")
							.option("max_partitions_per_insert_block","100")
							.option("max_block_size","1024")
							.option("max_memory_usage","800000000")
							.option("preferred_max_column_in_block_size_bytes","0")
							.option("createTableOptions", "ENGINE=Log()")
					        .mode(SaveMode.Append)
					        .save();
					}
				})
				.option("checkpointLocation", props.getProperty("spark.checkpoint.dir"))
				.start();
		
		query.awaitTermination();    

	}
}
