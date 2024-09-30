from pyspark.sql.types import BooleanType
import ZeekLog as log

class ZeekDns(object):
    SOURCE_NAME = 'dns'

    FIELD_NAME_CONVERSIONS = {
        'ts' : 'ts',
        'uid' : 'network_connection_uid',
        'id.orig_h' : 'network_source_ip',
        'id.orig_p' : 'network_source_port',
        'id.resp_h' : 'network_dest_ip',
        'id.resp_p' : 'network_dest_port',
        'proto' : 'network_protocol',
        'trans_id' : 'dns_trans_id',
        'query' : 'dns_query',
        'qclass' : 'dns_query_class_code',
        'qclass_name' : 'dns_query_class',
        'qtype' : 'dns_query_type_code',
        'qtype_name' : 'dns_query_type',
        'rcode' : 'dns_response_code',
        'rcode_name' : 'dns_reponse',
        'AA' : 'dns_aa',
        'TC' : 'dns_tc',
        'RD' : 'dns_rd',
        'RA' : 'dns_ra',
        'Z' :'dns_z',
        'answers' : 'dns_answers',
        'TTLs' : 'dns_ttls',
        'rejected' : 'dns_rejected'}

    @staticmethod
    def postProcess(dataframe):
        return ( dataframe.drop('dns_query_class_code').drop('dns_query_type_code').drop('dns_response_code')
           .withColumn('dns_aa', dataframe.dns_aa.cast(dataType=BooleanType()))
           .withColumn('dns_tc', dataframe.dns_tc.cast(dataType=BooleanType()))
           .withColumn('dns_rd', dataframe.dns_rd.cast(dataType=BooleanType()))
           .withColumn('dns_ra', dataframe.dns_ra.cast(dataType=BooleanType()))
           .withColumn('dns_z', dataframe.dns_z.cast(dataType=BooleanType()))
           .withColumn('dns_rejected', dataframe.dns_rejected.cast(dataType=BooleanType())) )

    @staticmethod
    def ingest(spark, log_bucket_path, dataset_name):
        log_file = "%s/%s_dns.log" %(log_bucket_path, dataset_name)
        zeek_log = log.ZeekLog(spark, log_file, ZeekDns.SOURCE_NAME, dataset_name, ZeekDns.FIELD_NAME_CONVERSIONS)
        return ZeekDns.postProcess(zeek_log.ingest())

  
