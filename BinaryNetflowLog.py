from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
from pyspark.sql.functions import lit
from pyspark.sql import functions as sql_functions 

class BinaryNetflowLog(object):
    SOURCE_NAME = 'binary_netflow'

    @staticmethod
    def construct_schema():
        return StructType([StructField('ip_src_addr', StringType(), True),
               StructField('ip_dst_addr', StringType(), True),
               StructField('network_protocol', StringType(), True),
               StructField('ip_src_port', IntegerType(), True), 
               StructField("ip_dst_port", IntegerType(), True), 
               StructField("connection_state", StringType(), True), 
               StructField("src_tos", IntegerType(), True), 
               StructField("dst_tos", IntegerType(), True), 
               StructField("src_win", IntegerType(), True), 
               StructField("dst_win", IntegerType(), True), 
               StructField("src_hops", IntegerType(), True), 
               StructField("dst_hops", IntegerType(), True), 
               StructField("start_time", StringType(), True), 
               StructField("last_time", StringType(), True), 
               StructField("src_ttl", IntegerType(), True), 
               StructField("dst_ttl", IntegerType(), True), 
               StructField("tcp_rtt", DoubleType(), True), 
               StructField("syn_ack", DoubleType(), True), 
               StructField("ack_dat", DoubleType(), True), 
               StructField("src_packets", IntegerType(), True), 
               StructField("dst_packets", IntegerType(), True), 
               StructField("src_bytes", LongType(), True), 
               StructField("dst_bytes", LongType(), True), 
               StructField("src_app_bytes", LongType(), True), 
               StructField("dst_app_bytes", LongType(), True), 
               StructField("flow_duration", DoubleType(), True), 
               StructField("flow_total_packets", LongType(), True), 
               StructField("flow_total_bytes", LongType(), True), 
               StructField("flow_total_app_bytes", LongType(), True), 
               StructField("flow_rate", DoubleType(), True),
               StructField("src_rate", DoubleType(), True), 
               StructField("dst_rate", DoubleType(), True), 
               StructField("flow_label", StringType(), True)])

    @staticmethod
    def ingest(spark, log_bucket_path, dataset_name):
        print("%s %s: Start" %(dataset_name, BinaryNetflowLog.SOURCE_NAME))
        log_schema = BinaryNetflowLog.construct_schema()
        log_file = "%s/%s_capture.binetflow.2format" %(log_bucket_path, dataset_name)
        dataframe = spark.read.csv(log_file, header=True, sep=',', schema=log_schema)
        return dataframe.withColumn('source', lit(BinaryNetflowLog.SOURCE_NAME)).withColumn("dataset_name", lit(dataset_name)).withColumn('start_time', sql_functions.to_timestamp(dataframe.start_time, 'yyyy/MM/dd HH:mm:ss.SSSSSS')).withColumn('last_time', sql_functions.to_timestamp(dataframe.last_time, 'yyyy/MM/dd HH:mm:ss.SSSSSS')).withColumn('infected', dataframe.flow_label.contains('Botnet'))
        
    
#    dataframe.write.mode('append').format("iceberg").saveAsTable("cybersec.ctu_botnet_capture")