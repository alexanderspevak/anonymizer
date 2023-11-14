use capnp::message::ReaderOptions;
use capnp::serialize;
use clickhouse::inserter::Inserter;
use clickhouse::Client;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, MessageSets};
use kafka::Error as KafkaError;
use log_row::LogRow;
use std::error::Error;
use std::result::Result;
use std::time::{Duration, Instant};
use std::{thread, time as systemTime};

pub mod ddl;
pub mod http_log_capnp;
pub mod log_row;

const TRY_AGAIN_AFTER: u64 = 10;
//FOR SOME REASON THIS NUMBER WORKS, NOT 60
const SAFE_TIME_CLICKHOUSE: u64 = 67;

fn get_kafka_consumer() -> Result<Consumer, KafkaError> {
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
                thread::sleep(systemTime::Duration::from_secs(TRY_AGAIN_AFTER));
            }
        }
    }
}

fn get_message_sets(consumer: &mut Consumer) -> MessageSets {
    loop {
        match consumer.poll() {
            Ok(m) => {
                if m.is_empty() {
                    return m;
                }
                println!("messages fetched from kafka");
                return m;
            }
            Err(e) => {
                println!("could not get messages from kafka. {:?}", e);
                println!("retry in {} secs", TRY_AGAIN_AFTER);
                thread::sleep(Duration::from_secs(TRY_AGAIN_AFTER))
            }
        }
    }
}

async fn commit_safely(inserter: &mut Inserter<LogRow>, consumer: &mut Consumer) {
    loop {
        match inserter.commit().await {
            Ok(q) => {
                println!(
                    "messages sent to clickhouse. Num: {} {}",
                    q.entries, q.transactions
                );
                break;
            }
            Err(err) => {
                println!("could not send messages to clickhouse. {:?}", err,);
                println!("retry in {} secs", TRY_AGAIN_AFTER);
                thread::sleep(Duration::from_secs(TRY_AGAIN_AFTER))
            }
        }
    }
    loop {
        match consumer.commit_consumed() {
            Ok(_) => {
                println!("messages marked as consumed in kafka");
                break;
            }
            Err(err) => {
                println!("could not mark messages as consumed in kafka. {:?}", err,);
                println!("retry in {} secs", TRY_AGAIN_AFTER);
                thread::sleep(Duration::from_secs(TRY_AGAIN_AFTER))
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = Client::default().with_url("http://localhost:8124");

    client.query(ddl::CREATE_TABLE_HTTP_LOG).execute().await?;
    println!("http_log table created");

    thread::sleep(Duration::from_secs(SAFE_TIME_CLICKHOUSE));

    client
        .query(ddl::CREATE_MATERIALIZED_VIEW_AGGREGATED_HTTP)
        .execute()
        .await?;

    println!("view created");

    let mut inserter = client.inserter("http_log")?;
    let mut consumer = get_kafka_consumer()?;
    let mut start = Instant::now();

    loop {
        let message_sets = get_message_sets(&mut consumer);
        for ms in message_sets.iter() {
            for m in ms.messages() {
                let reader = serialize::read_message(m.value, ReaderOptions::new()).unwrap();

                let reader = reader
                    .get_root::<http_log_capnp::http_log_record::Reader>()
                    .unwrap();
                let mut row = LogRow::new(reader).unwrap();
                row.anonymize();
                println!("writing message to batch {:?}", row);
                inserter.write(&row).await?;
            }
        }

        if start.elapsed() >= Duration::from_secs(SAFE_TIME_CLICKHOUSE) {
            commit_safely(&mut inserter, &mut consumer).await;
            start = Instant::now();
        }
    }
}
