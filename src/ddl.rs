pub const CREATE_TABLE_HTTP_LOG: &str = r"
            CREATE TABLE IF NOT EXISTS http_log (
                timestamp DateTime64,
                resource_id UInt64,
                bytes_sent UInt64,
                request_time_milli UInt64,
                response_status UInt16,
                cache_status String,
                method String,
                remote_addr String,
                url String
            )
            ENGINE = MergeTree
          ORDER BY timestamp
    ";

pub const CREATE_MATERIALIZED_VIEW_AGGREGATED_HTTP: &str =
    "CREATE MATERIALIZED VIEW IF NOT EXISTS aggregated_http
       ENGINE = AggregatingMergeTree()
       ORDER BY (resource_id, response_status, cache_status, remote_addr)
       AS SELECT 
           resource_id,
           response_status,
           cache_status,
           remote_addr,
           sum(bytes_sent) as total_bytes_sent,
           count() as total_requests,
           avg(request_time_milli) as average_request_time
       FROM http_log
       GROUP BY resource_id, response_status, cache_status, remote_addr;";
