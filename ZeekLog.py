import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import TimestampType, StructType, StructField, StringType, DoubleType, LongType, IntegerType
from pyspark.sql import functions as sql_functions  
      
class ZeekLog(object):

    SEPARATOR_PREFIX = '#separator'
    EMPTY_FIELD_PREFIX =  '#empty_field'
    UNSET_FIELD_PREFIX = '#unset_field'
    FIELD_NAME_PREFIX = "#fields"
    FIELD_TYPE_PREFIX = "#types"

    ZEEK_TYPE_TO_SQL_TYPE_MAP = {
        'time' : DoubleType(),
        'port' : IntegerType(),
        'count' : IntegerType()
    }
    
    def __init__(self, spark, log_path, log_source_name, dataset_name, field_conversions):
        self.spark = spark
        self.logPath = log_path
        self.fieldConversions = field_conversions
        self.fieldSeparator = None
        self.logSourceName = log_source_name
        self.datasetName = dataset_name
        
    def construct_schema(self):
        headers = {ZeekLog.SEPARATOR_PREFIX: '\\x09', ZeekLog.EMPTY_FIELD_PREFIX: '(empty)', ZeekLog.UNSET_FIELD_PREFIX: '-'}
        first_lines = self.spark.read.text(self.logPath).limit(8)
        for header_row in first_lines.collect():
            header_string = header_row.value
            if (header_string.startswith("#")):
                name_value = re.split("\s", header_string, 1)
                headers[name_value[0]] = name_value[1]

        if (ZeekLog.EMPTY_FIELD_PREFIX in headers):
            self.emptyFieldValue = headers[ZeekLog.EMPTY_FIELD_PREFIX]

        if (ZeekLog.UNSET_FIELD_PREFIX in headers):
            self.unsetFieldValue = headers[ZeekLog.UNSET_FIELD_PREFIX]
    
        field_names = headers[ZeekLog.FIELD_NAME_PREFIX].split(self.fieldSeparator)
        field_types = headers[ZeekLog.FIELD_TYPE_PREFIX].split(self.fieldSeparator)
        self.fieldSeparator = chr(int(headers[ZeekLog.SEPARATOR_PREFIX].replace("\\x", ""), 16))
    
        field_structs=[]
        for i in range(0, len(field_names)):
            zeek_field_name = field_names[i]
            field_type = field_types[i]
            sql_type = StringType()
            if (field_type in ZeekLog.ZEEK_TYPE_TO_SQL_TYPE_MAP):
                sql_type = ZeekLog.ZEEK_TYPE_TO_SQL_TYPE_MAP[field_type]
            field_name = zeek_field_name
            if (zeek_field_name in self.fieldConversions):
                field_name = self.fieldConversions[zeek_field_name]
            field_structs.append(StructField(field_name, sql_type, True))
            
        return StructType(field_structs)

    def ingest(self):
        print("%s %s: Start" %(self.datasetName, self.logSourceName))
        log_schema = self.construct_schema()

        dataframe = self.spark.read.csv(self.logPath, header=True, sep=self.fieldSeparator, schema=log_schema, comment='#', emptyValue=self.emptyFieldValue)
        return dataframe.withColumn('source', lit(self.logSourceName)).withColumn("dataset_name", lit(self.datasetName)).withColumn('ts', sql_functions.to_timestamp(dataframe.ts.cast(dataType=TimestampType())))      