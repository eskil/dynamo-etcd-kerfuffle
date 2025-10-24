// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use dynamo_runtime::{
    DistributedRuntime, Result, Runtime, Worker, logging, pipeline::PushRouter,
    protocols::annotated::Annotated, stream::StreamExt,
};
use hello_world::DEFAULT_NAMESPACE;

fn main() -> Result<()> {
    logging::init();
    let worker = Worker::from_settings()?;
    worker.execute(app)
}

async fn app(runtime: Runtime) -> Result<()> {
    let distributed = DistributedRuntime::from_settings(runtime.clone()).await?;

    let client = distributed
        .namespace(DEFAULT_NAMESPACE)?
        .component("backend")?
        .endpoint("generate")
        .client()
        .await?;
    client.wait_for_instances().await?;
    let router =
        PushRouter::<String, Annotated<String>>::from_client(client, Default::default()).await?;

    let mut stream = router.random("hello world".to_string().into()).await?;

    while let Some(resp) = stream.next().await {
        println!("{:?}", resp);
    }

    runtime.shutdown();

    Ok(())
}
