use capnp::message::ReaderOptions;
use capnp::serialize;
use clickhouse::insert::Insert;
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

const TRY_AGAIN_AFTER: u64 = 15;
//FOR SOME REASON THIS NUMBER WORKS, NOT 60
const SAFE_TIME_CLICKHOUSE: u64 = 67;

async fn write_to_batch_safely(insert: &mut Insert<LogRow>, row: &LogRow) {
    loop {
        match insert.write(row).await {
            Ok(_) => {
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

async fn safety_commit(client: &Client, rows: &[LogRow], consumer: &mut Consumer) {
    loop {
        let mut inserter: Insert<LogRow> = client.insert("http_log").unwrap();

        for row in rows {
            write_to_batch_safely(&mut inserter, row).await;
        }

        match inserter.end().await {
            Ok(_) => {
                println!("safety commit to clickhouse");
                commit_to_kafka(consumer).await;
                return;
            }
            Err(err) => {
                println!("error writing to socket {:?}", err);
                println!("retry in {} secs", TRY_AGAIN_AFTER);
                thread::sleep(Duration::from_secs(TRY_AGAIN_AFTER));
            }
        }
    }
}

async fn process_messages(consumer: &mut Consumer, client: &Client) {
    let mut insert: Insert<LogRow> = client.insert("http_log").unwrap();
    let mut start = Instant::now();
    let mut safety_container = Vec::new();

    loop {
        let message_sets = producer::get_message_sets(consumer);
        for ms in message_sets.iter() {
            for m in ms.messages() {
                let reader = match serialize::read_message(m.value, ReaderOptions::new()) {
                    Ok(reader) => reader,
                    Err(err) => {
                        println!("unable to serialize message. {:?}", err);
                        continue;
                    }
                };

                let reader = match reader.get_root::<http_log_capnp::http_log_record::Reader>() {
                    Ok(reader) => reader,
                    Err(err) => {
                        println!("Can not identify message type. {:?}", err);
                        continue;
                    }
                };

                let mut row = match LogRow::new(reader) {
                    Ok(row) => row,
                    Err(err) => {
                        println!("invalid message from kafka. {:?}", err);
                        continue;
                    }
                };

                row.anonymize();

                safety_container.push(row.clone());
                write_to_batch_safely(&mut insert, &row).await;
            }
        }
        if start.elapsed() >= Duration::from_secs(SAFE_TIME_CLICKHOUSE) {
            if let Err(err) = commit(insert, consumer).await {
                println!("error commiting data {:?}", err);
                safety_commit(client, &safety_container, consumer).await;
            };
            start = Instant::now();
            safety_container = Vec::new();
            insert = client.insert("http_log").unwrap();
        }
    }
}

async fn commit(insert: Insert<LogRow>, consumer: &mut Consumer) -> Result<(), Box<dyn Error>> {
    insert.end().await?;
    println!("data commited to clickhouse");
    Ok(commit_to_kafka(consumer).await)
}

async fn commit_to_kafka(consumer: &mut Consumer) {
    loop {
        match consumer.commit_consumed() {
            Ok(_) => return,
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

    let mut consumer = producer::get_kafka_consumer()?;

    process_messages(&mut consumer, &client).await;

    Ok(())
}
