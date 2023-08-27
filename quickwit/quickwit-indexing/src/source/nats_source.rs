use std::default;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use async_nats::{connect, Client, ServerAddr, Subscriber};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use quickwit_actors::{ActorContext, ActorExitStatus, Mailbox};
use quickwit_config::NatsSourceParams;
use quickwit_metastore::checkpoint::{
    PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta,
};
use serde_json::{json, Value as JsonValue};
use tracing::log::warn;

use crate::actors::DocProcessor;
use crate::source::{
    BatchBuilder, Source, SourceActor, SourceContext, SourceExecutionContext, TypedSourceFactory,
};

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
    queue_group: String,
    state: NatsSourceState,
}

impl NatsSource {
    pub async fn try_new(
        ctx: Arc<SourceExecutionContext>,
        params: NatsSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        let topic = params.subject.clone();
        let queue_group = params.queue_group.clone();
        let client = connect(
            params
                .address
                .iter()
                .map(|url| url.parse())
                .collect::<Result<Vec<ServerAddr>, _>>()?,
        )
        .await?;
        let subscriber = client
            .queue_subscribe(topic.clone(), queue_group.clone())
            .await?;
        let state = NatsSourceState::default();
        Ok(NatsSource {
            client,
            subscriber,
            topic,
            queue_group,
            state,
        })
    }
    fn process_message(
        &mut self,
        message: async_nats::Message,
        batch: &mut BatchBuilder,
    ) -> anyhow::Result<()> {
        let payload = message.payload;
        self.add_doc_to_batch(
            &message.subject,
            // current_position,
            payload,
            batch,
        )?;
        Ok(())
    }

    fn add_doc_to_batch(
        &mut self,
        topic: &str,
        // msg_position: Position,
        doc: Bytes,
        batch: &mut BatchBuilder,
    ) -> anyhow::Result<()> {
        if doc.is_empty() {
            warn!("Message received from queue was empty.");
            self.state.num_invalid_messages += 1;
            return Ok(());
        }

        let partition = PartitionId::from(topic);
        let num_bytes = doc.len() as u64;

        let current_position = batch.push(doc);

        self.state.num_bytes_processed += num_bytes;
        self.state.num_messages_processed += 1;

        Ok(())
    }
}

#[async_trait]
impl Source for NatsSource {
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let _now = Instant::now();
        let mut batch = BatchBuilder::default();
        let deadline = tokio::time::sleep(*quickwit_actors::HEARTBEAT / 2);
        tokio::pin!(deadline);

        // Read the message and send
        loop {
            tokio::select! {
                message = self.subscriber.next() => {
                    let message = message
                        .ok_or_else(|| ActorExitStatus::from(anyhow!("Message couldnt be read.")))
                        .unwrap_or_else(|err| {
                            ctx.record_progress();
                            panic!("Message couldnt be read. {}", err)
                        });
                    self.process_message(message, &mut batch).map_err(ActorExitStatus::from)?;

                    if batch.num_bytes >= BATCH_NUM_BYTES_LIMIT {
                        break;
                    }
                }
                _ = &mut deadline => {
                    break;
                }
            }
            ctx.record_progress();
        }

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
