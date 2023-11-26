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

async fn safety_commit(client: &Client, rows: &[LogRow]) {
    loop {
        let mut inserter: Inserter<LogRow> = client.inserter("http_log").unwrap();

        for row in rows {
            write_to_batch_safely(&mut inserter, row).await;
        }

        match inserter.end().await {
            Ok(_) => return,
            Err(err) => {
                println!("error writing to socket {:?}", err);
                println!("retry in {} secs", TRY_AGAIN_AFTER);
                thread::sleep(Duration::from_secs(TRY_AGAIN_AFTER));
            }
        }
    }
}

async fn process_messages(consumer: &mut Consumer, client: &Client) {
    let mut inserter: Inserter<LogRow> = client.inserter("http_log").unwrap();
    let mut start = Instant::now();
    let mut safety_container = Vec::new();

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

                safety_container.push(row.clone());
                write_to_batch_safely(&mut inserter, &row).await;
            }
        }
        if start.elapsed() >= Duration::from_secs(SAFE_TIME_CLICKHOUSE) {
            if let Err(_) = commit(inserter, consumer).await {
                safety_commit(client, &safety_container).await;
            };
            start = Instant::now();
            safety_container = Vec::new();
            inserter = client.inserter("http_log").unwrap();
        }
    }
}

async fn commit(inserter: Inserter<LogRow>, consumer: &mut Consumer) -> Result<(), Box<dyn Error>> {
    inserter.end().await?;
    println!("messages written to clickhouse");
    loop {
        match consumer.commit_consumed() {
            Ok(_) => return Ok(()),
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
