use std::sync::Arc;
use std::time::{Duration};
use serde_json::{json, Value as JsonValue};
use async_trait::async_trait;
use quickwit_config::NatsSourceParams;
use crate::source::{
    Source, SourceActor, SourceContext, SourceExecutionContext, TypedSourceFactory,
};
use quickwit_metastore::checkpoint::{
    PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta,
};
use crate::actors::DocProcessor;
use quickwit_actors::{ActorContext, ActorExitStatus, Mailbox};


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


pub struct NatsSource {

}

impl NatsSource {
    pub async fn try_new(
        ctx: Arc<SourceExecutionContext>,
        params: NatsSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        Ok(NatsSource{})
    }
}

#[async_trait]
impl Source for NatsSource {
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext
    ) -> Result<Duration, ActorExitStatus> {
        Ok(Duration::default())
    }

    async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        _ctx: &ActorContext<SourceActor>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn name(&self) -> String {
        "".to_string()
    }

    fn observable_state(&self) -> JsonValue {
        json!({})
    }
}
