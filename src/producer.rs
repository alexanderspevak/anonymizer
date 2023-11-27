use kafka::Error as KafkaError;

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, MessageSets};
use std::time::Duration;
use std::{thread, time as systemTime};

pub fn get_kafka_consumer() -> Result<Consumer, KafkaError> {
    let mut counter = 0;
    loop {
        match Consumer::from_hosts(vec!["localhost:9092".to_owned()])
            .with_topic("http_log".to_string())
            .with_fallback_offset(FetchOffset::Earliest)
            .with_group("http_log".to_string())
            .with_offset_storage(Some(GroupOffsetStorage::Kafka))
            .create()
        {
            Ok(consumer) => return Ok(consumer),
            Err(error) => {
                if counter > 10 {
                    return Err(error);
                }
                counter += 1;
                println!("failed to connect to to kafka. Restarting.  {:?}", error);
                thread::sleep(systemTime::Duration::from_secs(super::TRY_AGAIN_AFTER));
            }
        }
    }
}

pub fn get_message_sets(consumer: &mut Consumer) -> MessageSets {
    loop {
        match consumer.poll() {
            Ok(m) => {
                if m.is_empty() {
                    return m;
                }
                // println!("messages fetched from kafka");
                return m;
            }
            Err(e) => {
                println!("could not get messages from kafka. {:?}", e);
                println!("retry in {} secs", super::TRY_AGAIN_AFTER);
                thread::sleep(Duration::from_secs(super::TRY_AGAIN_AFTER))
            }
        }
    }
}
