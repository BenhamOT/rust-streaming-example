use std::error::Error;
use std::str::FromStr;
use std::time::Duration;
use iggy::client::{Client, UserClient};
use iggy::clients::client::IggyClient;
use iggy::users::login_user::LoginUser;
use iggy::identifier::Identifier;
use iggy::streams::create_stream::CreateStream;
use iggy::topics::create_topic::CreateTopic;
use iggy::messages::send_messages::{Message, Partitioning, SendMessages};
use tracing::{info, warn};
use tokio::time::sleep;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const PARTITION_ID: u32 = 1;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    let mut client = IggyClient::default();
    client.connect().await?;
    client.login_user(
        &LoginUser {
            username: "iggy".to_string(),
            password: "iggy".to_string()
        }
    ).await?;
    init_system(&client).await;
    produce_messages(&client).await?;
    Ok(())
}

async fn init_system(client: &dyn Client) {
    match client
        .create_stream(&CreateStream {
            stream_id: STREAM_ID,
            name: "sample-stream".to_string(),
        })
        .await
    {
        Ok(_) => info!("Stream was created."),
        Err(_) => warn!("Stream already exists and was not recreated."),
    }

    match client
        .create_topic(&CreateTopic {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: TOPIC_ID,
            partitions_count: 1,
            name: "sample-topic".to_string(),
            message_expiry: None,
        })
        .await
    {
        Ok(_) => info!("Topic was created."),
        Err(_) => warn!("Topic already exists and was not recreated."),
    }
}

async fn produce_messages(client: &dyn Client) -> Result<(), Box<dyn Error>> {
    let interval = Duration::from_millis(500);
    info!(
        "Messages will be sent to stream: {}, topic: {}, partition: {} with interval {} ms.",
        STREAM_ID,
        TOPIC_ID,
        PARTITION_ID,
        interval.as_millis()
    );
    let mut current_id = 0;
    let messages_per_batch = 10;
    loop {
        let mut messages = Vec::new();
        for _ in 0..messages_per_batch {
            current_id += 1;
            let payload = format!("message-{current_id}");
            let message = Message::from_str(&payload)?;
            messages.push(message);
        }
        client
            .send_messages(&mut SendMessages {
                stream_id: Identifier::numeric(STREAM_ID)?,
                topic_id: Identifier::numeric(TOPIC_ID)?,
                partitioning: Partitioning::partition_id(PARTITION_ID),
                messages,
            })
            .await?;
        info!("Sent {messages_per_batch} message(s).");
        sleep(interval).await;
    }
}