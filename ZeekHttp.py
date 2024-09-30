from pyspark.sql.types import BooleanType
import ZeekLog as log

class ZeekHttp(object):
    SOURCE_NAME = 'http'

#status_msg	info_code	info_msg	filename	tags	username	password	proxied	orig_fuids	orig_mime_types	resp_fuids	resp_mime_types
    FIELD_NAME_CONVERSIONS = {
        'ts' : 'ts',
        'uid' : 'network_connection_uid',
        'id.orig_h' : 'network_source_ip',
        'id.orig_p' : 'network_source_port',
        'id.resp_h' : 'network_dest_ip',
        'id.resp_p' : 'network_dest_port',
        'proto' : 'network_protocol',
        'trans_id' : 'http_trans_id',
        'method' : 'http_method',
        'host' : 'http_host',
        'uri' : 'http_uri',
        'referrer': 'http_referrer',
        'user_agent' : 'http_user_agent',
        'status_code' : 'http_status_code',
        'request_body_len' : "http_request_body_len",
        'response_body_len' : 'http_response_body_len',
        'info_code' : 'http_info_code',
        'filename': 'http_filename',
        'orig_mime_types': 'http_source_mime',
        'resp_mime_types': 'http_dest_mime'
    }

    @staticmethod
    def ingest(spark, log_bucket_path, dataset_name):
        log_file = "%s/%s_http.log" %(log_bucket_path, dataset_name)
        zeek_log = log.ZeekLog(spark, log_file, ZeekHttp.SOURCE_NAME, dataset_name, ZeekHttp.FIELD_NAME_CONVERSIONS)
        raw_dataframe = zeek_log.ingest()
        return ZeekHttp.postProcess(raw_dataframe)

    @staticmethod
    def postProcess(dataframe):
        return dataframe.drop('info_msg').drop('tags').drop('password').drop('proxied').drop('orig_fuids').drop('resp_fuids').drop('status_msg').drop('trans_depth')

