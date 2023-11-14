use super::http_log_capnp::http_log_record;
use clickhouse::{error::Result, Row};
use serde::{Deserialize, Serialize};
use std::error::Error;

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct LogRow {
    pub timestamp: u64,
    pub resource_id: u64,
    pub bytes_sent: u64,
    pub request_time_milli: u64,
    pub response_status: u16,
    pub cache_status: String,
    pub method: String,
    pub remote_addr: String,
    pub url: String,
}

impl LogRow {
    pub fn new(reader: http_log_record::Reader) -> Result<LogRow, Box<dyn Error>> {
        let timestamp = reader.get_timestamp_epoch_milli() / 1000;
        // let timestamp = timestamp as u16;
        let resource_id = reader.get_resource_id();
        let bytes_sent = reader.get_bytes_sent();
        let request_time_milli = reader.get_request_time_milli();
        let response_status = reader.get_response_status();
        let cache_status = reader.get_cache_status()?.to_owned();
        let method = reader.get_method()?.to_owned();
        let remote_addr = reader.get_remote_addr()?.to_owned();
        let url = reader.get_url()?.to_owned();

        return Ok(LogRow {
            timestamp,
            resource_id,
            bytes_sent,
            request_time_milli,
            response_status,
            cache_status,
            method,
            remote_addr,
            url,
        });
    }

    pub fn anonymize(&mut self) {
        let mut parts: Vec<&str> = self.remote_addr.split('.').collect();
        if let Some(_) = parts.pop() {
            parts.push("X");
        }
        self.remote_addr = parts.join(".");
    }
}
