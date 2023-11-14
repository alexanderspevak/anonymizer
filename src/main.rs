use capnp::message::ReaderOptions;
use capnp::serialize;
use clickhouse::inserter::Inserter;
use clickhouse::Client;
use kafka::consumer::Consumer;
use log_row::LogRow;
use std::error::Error;
use std::result::Result;
use std::thread;
use std::time::{Duration, Instant};

pub mod ddl;
pub mod http_log_capnp;
pub mod log_row;
pub mod producer;

const TRY_AGAIN_AFTER: u64 = 10;
//FOR SOME REASON THIS NUMBER WORKS, NOT 60
const SAFE_TIME_CLICKHOUSE: u64 = 67;

async fn write_to_batch_safely(inserter: &mut Inserter<LogRow>, row: &LogRow) {
    loop {
        match inserter.write(row).await {
            Ok(_) => {
                println!("row written {:?}", row);
                return;
            }
            Err(err) => {
                println!("error writing to socket {:?}", err);
                println!("retry in {} secs", TRY_AGAIN_AFTER);
                thread::sleep(Duration::from_secs(TRY_AGAIN_AFTER))
            }
        }
    }
}

async fn process_messages(inserter: &mut Inserter<LogRow>, consumer: &mut Consumer) {
    let mut start = Instant::now();

    loop {
        let message_sets = producer::get_message_sets(consumer);
        for ms in message_sets.iter() {
            for m in ms.messages() {
                let reader = serialize::read_message(m.value, ReaderOptions::new()).unwrap();

                let reader = reader
                    .get_root::<http_log_capnp::http_log_record::Reader>()
                    .unwrap();
                let mut row = LogRow::new(reader).unwrap();
                row.anonymize();
                write_to_batch_safely(inserter, &row).await;
            }
        }

        if start.elapsed() >= Duration::from_secs(SAFE_TIME_CLICKHOUSE) {
            commit_safely(inserter, consumer).await;
            start = Instant::now();
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
    let mut consumer = producer::get_kafka_consumer()?;

    process_messages(&mut inserter, &mut consumer).await;

    Ok(())
}
