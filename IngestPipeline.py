import ZeekDns
import ZeekDns as dns
import ZeekHttp as http
import BinaryNetflowLog as binet

from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


class IngestPipeline(object):

    @staticmethod
    def createSparkSession(log_bucket_path):
        bucket_components = urlparse(log_bucket_path)
        hadoop_file_system_location = "%s://%s" % (bucket_components.scheme, bucket_components.netloc)
        
        return (SparkSession
           .builder
           .appName("CTU13 Ingest Spark").config("spark.jars", "/opt/spark/optional-lib/iceberg-spark-runtime.jar,/opt/spark/optional-lib/iceberg-hive-runtime.jar")
           .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
           .config("spark.sql.catalog.spark_catalog.type", "hive")
           .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
           .config("spark.sql.iceberg.check-ordering", "false")
           .config("spark.hadoop.iceberg.engine.hive.enabled", "true")
           .config("spark.yarn.access.hadoopFileSystems", hadoop_file_system_location)
           .getOrCreate()
        )

    @staticmethod
    def writeDataFrame(spark, dataset_name, source, full_table_name, dataframe):

        print("%s %s: Writing to table %s" %(dataset_name, source, full_table_name))

        table_name_parts = full_table_name.split('.')
        database_name = table_name_parts[0]
        table_name = table_name_parts[1]
        new_table = False
        if (spark._jsparkSession.catalog().tableExists(database_name, table_name)):
            print("Table %s exists. Merging columns" % (full_table_name))
            existing_table = spark.table(full_table_name).limit(0)
            for existing_field in existing_table.schema.fields:
                existing_field_name = existing_field.name
                if (existing_field_name not in dataframe.schema.fieldNames()):
                     dataframe = dataframe.withColumn(existing_field_name, lit(None).cast(existing_field.dataType))
            print("Appending to table %s" % (full_table_name))
            dataframe.writeTo(full_table_name).option("mergeSchema","true").append()
        else:
            print("Creating table %s" % (full_table_name))
            dataframe.write.mode('append').format("iceberg").saveAsTable(full_table_name)
            spark.sql("ALTER TABLE `%s`.`%s` SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')" % (database_name, table_name))
                        
        print("%s %s: Done" %(dataset_name, source))


    @staticmethod
    def ingest(log_bucket_path):
        database_name = "cyber_ctu"
        binetflow_table = ".".join([database_name, "ctu_binetflow"])
        zeek_table = ".".join([database_name, "ctu_zeek"])
        
        print("Ingesting logs: %s" %(log_bucket_path))
        
        spark = IngestPipeline.createSparkSession(log_bucket_path)
        
        for scenario_number in range(42,54):
            dataset_name = 'CTU-Malware-Capture-Botnet-%d' % (scenario_number)

            print("Dataset %s" % (dataset_name))

            binet_log = binet.BinaryNetflowLog.ingest(spark, log_bucket_path, dataset_name)
            IngestPipeline.writeDataFrame(spark, dataset_name, binet.BinaryNetflowLog.SOURCE_NAME, binetflow_table, binet_log)
        
            dns_dataframe = dns.ZeekDns.ingest(spark, log_bucket_path, dataset_name)
            IngestPipeline.writeDataFrame(spark, dataset_name, dns.ZeekDns.SOURCE_NAME, zeek_table, dns_dataframe)
      
            http_dataframe = http.ZeekHttp.ingest(spark, log_bucket_path, dataset_name)
            IngestPipeline.writeDataFrame(spark, dataset_name, http.ZeekHttp.SOURCE_NAME, zeek_table, http_dataframe)

