use std::error::Error;
use std::time::Duration;
use iggy::client::{Client, UserClient};
use iggy::clients::client::IggyClient;
use iggy::consumer::Consumer;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::{PollingStrategy, PollMessages};
use iggy::models::messages::Message;
use iggy::users::login_user::LoginUser;
use tokio::time::sleep;
use tracing::info;

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const PARTITION_ID: u32 = 1;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    let mut client = IggyClient::default();
    client.connect().await?;
    client.login_user(&LoginUser {
        username: "iggy".to_string(),
        password: "iggy".to_string(),
    }).await?;
    consume_message(& client).await
}

async fn consume_message(client: &dyn Client) -> Result<(), Box<dyn Error>> {
    let interval = Duration::from_millis(500);
    info!(
        "Message will be consumed from stream: {}, topic: {} and partition: {}",
        STREAM_ID,
        TOPIC_ID,
        PARTITION_ID
    );
    let mut offset = 0;
    let messages_per_batch = 10;
    loop {
        let polled_messages = client.poll_messages(&PollMessages {
            consumer: Consumer::default(),
            stream_id: Identifier::numeric(STREAM_ID)?,
            topic_id: Identifier::numeric(TOPIC_ID)?,
            partition_id: Some(PARTITION_ID),
            strategy: PollingStrategy::offset(offset),
            count: messages_per_batch,
            auto_commit: false,
        }).await?;

        if polled_messages.messages.is_empty() {
            info!("No messages found.");
            sleep(interval).await;
            continue;
        }
        offset += polled_messages.messages.len() as u64;
        for message in polled_messages.messages {
            handle_message(&message).await?;
        }
        sleep(interval).await;
    }
}

async fn handle_message(message: &Message) -> Result<(), Box<dyn Error>> {
    let payload = std::str::from_utf8(&message.payload)?;
    info!("Handling message at offset: {}, payload: {} ...", message.offset, payload);
    Ok(())
}