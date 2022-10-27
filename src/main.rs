use clap::Parser;
use model::User;
use rdkafka::{consumer::{StreamConsumer, Consumer, CommitMode}, ClientConfig, config::RDKafkaLogLevel, Message};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    topic: String,
    #[arg(short, long, default_value="localhost:9092")]
    broker: String,
    #[arg(short, long, default_value="mygroup")]
    group_id: String
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    receive_loop(&args).await;
}

async fn receive_loop(args: &Args) {
    let topics: Vec<&str> = vec![&args.topic];
    let consumer: StreamConsumer<_> = ClientConfig::new()
        .set("group.id", &args.group_id)
        .set("bootstrap.servers", &args.broker)
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&topics[..]).expect("Cannot subscribe.");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                match msg.payload_view::<[u8]>() {
                    Some(result) => {
                        match User::deserialize(result.unwrap()) {
                            Ok(user) => println!("Received: {:?}", user),
                            Err(_) => println!("Cannot deserialize. Ignored."),
                        }
                    },
                    None => (),
                };
                consumer.commit_message(&msg, CommitMode::Async).unwrap();
            },
            Err(e) => println!("Kafka error: {}", e),
        }
    }
}