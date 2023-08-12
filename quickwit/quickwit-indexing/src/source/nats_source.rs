use std::default;
use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::anyhow;
use bytes::Bytes;
use futures::StreamExt;
use serde_json::{json, Value as JsonValue};
use async_trait::async_trait;
use quickwit_config::NatsSourceParams;
use tracing::log::warn;
use crate::source::{
    BatchBuilder, Source, SourceActor, SourceContext, SourceExecutionContext, TypedSourceFactory,
};
use quickwit_metastore::checkpoint::{
    PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta,
};
use crate::actors::DocProcessor;
use quickwit_actors::{ActorContext, ActorExitStatus, Mailbox};
use async_nats::{connect, Client, Subscriber};

const BATCH_NUM_BYTES_LIMIT: u64 = 5_000_000;

pub struct NatsSourceFactory;

#[async_trait]
impl TypedSourceFactory for NatsSourceFactory {
    type Source = NatsSource;
    type Params = NatsSourceParams;

    async fn typed_create_source(
        ctx: Arc<SourceExecutionContext>,
        params: NatsSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        NatsSource::try_new(ctx, params, checkpoint).await
    }
}

#[derive(Debug, Default)]
struct NatsSourceState {
    num_messages_processed: usize,
    num_bytes_processed: u64,
    num_skipped_messages: usize,
    num_invalid_messages: usize,
}

impl NatsSourceState {
    fn default() -> Self {
        Self {
            num_messages_processed: 0,
            num_bytes_processed: 0,
            num_skipped_messages: 0,
            num_invalid_messages: 0,
        }
    }
}

pub struct NatsSource {
    client: Client,
    subscriber: Subscriber,
    topic: String,
    state: NatsSourceState,
}

impl NatsSource {
    pub async fn try_new(
        ctx: Arc<SourceExecutionContext>,
        params: NatsSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        let topic = "nats_source".to_string();
        let client = connect("localhost:4222").await?;
        let subscriber = client.subscribe(topic.clone()).await?;
        Ok(NatsSource{
            client: client,
            subscriber: subscriber,
            topic: topic,
        })
    }
}

#[async_trait]
impl Source for NatsSource {
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext
    ) -> Result<Duration, ActorExitStatus> {
        // Read the message and send
        self.subscriber.next().await;

        Ok(Duration::default())
    }

    async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        _ctx: &ActorContext<SourceActor>,
    ) -> anyhow::Result<()> {
        // nothing to do
        Ok(())
    }

    fn name(&self) -> String {
        format!(
            // todo change with sourceID
            "NatsSource{{source_id={}}}",
            self.topic
        )
    }

    fn observable_state(&self) -> JsonValue {
        json!({
            "num_messages_processed": self.state.num_messages_processed,
            "num_bytes_processed": self.state.num_bytes_processed,
            "num_skipped_messages": self.state.num_skipped_messages,
            "num_invalid_messages": self.state.num_invalid_messages,
            "topic": self.topic,
        })
    }
}
