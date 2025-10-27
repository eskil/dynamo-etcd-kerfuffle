// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::time::Duration;

/// Create a [`Lease`] with a given time-to-live (TTL) attached to the [`CancellationToken`].
pub async fn create_lease(
    mut lease_client: LeaseClient,
    ttl: u64,
    token: CancellationToken,
) -> Result<Lease> {
    debug_println!(BLUE, "[CREATE_LEASE]", RESET, "Creating lease ttl={}", ttl);
    
    let lease = lease_client.grant(ttl as i64, None).await?;
    debug_println!(BLUE, "[CREATE_LEASE]", RESET, "Lease granted lease_id={}, ttl={}", lease.id(), lease.ttl());

    let id = lease.id() as u64;
    let ttl = lease.ttl() as u64;
    let child = token.child_token();
    let clone = token.clone();

    debug_println!(BLUE, "[CREATE_LEASE]", RESET, "Spawning keep-alive task lease_id={}", id);
    tokio::spawn(async move {
        debug_println!(BLUE, "[CREATE_LEASE]", RESET, "Keep-alive task started lease_id={}", id);
        
        // Add a panic hook to catch any panics
        std::panic::set_hook(Box::new(move |panic_info| {
            debug_println!(RED, "[CREATE_LEASE]", RESET, "PANIC in keep-alive task lease_id={}: {:?}", id, panic_info);
        }));
        
        match keep_alive(lease_client, id, ttl, child).await {
            Ok(_) => {
                debug_println!(GREEN, "[CREATE_LEASE]", RESET, "Keep-alive task EXITED successfully lease_id={}", id);
                tracing::trace!("keep alive task exited successfully");
            },
            Err(e) => {
                debug_println!(RED, "[CREATE_LEASE]", RESET, "Keep-alive task FAILED lease_id={}: {}", id, e);
                tracing::error!(
                    error = %e,
                    "Unable to maintain lease. Check etcd server status"
                );
                token.cancel();
            }
        }
        
        debug_println!(BLUE, "[CREATE_LEASE]", RESET, "Keep-alive task completely finished lease_id={}", id);
    });

    debug_println!(BLUE, "[CREATE_LEASE]", RESET, "Returning lease with lease_id={}", id);
    Ok(Lease {
        id,
        cancel_token: clone,
    })
}

/// Revoke a lease given its lease id. A wrapper over etcd_client::LeaseClient::revoke
pub async fn revoke_lease(mut lease_client: LeaseClient, lease_id: u64) -> Result<()> {
    match lease_client.revoke(lease_id as i64).await {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::warn!("failed to revoke lease: {:?}", e);
            Err(e.into())
        }
    }
}

/// Task to keep leases alive.
///
/// If this task returns an error, the cancellation token will be invoked on the runtime.
pub async fn keep_alive(
    client: LeaseClient,
    lease_id: u64,
    ttl: u64,
    token: CancellationToken,
) -> Result<()> {
    let mut ttl = ttl;
    let mut deadline = create_deadline(ttl)?;

    let mut client = client;
    let (mut heartbeat_sender, mut heartbeat_receiver) = client.keep_alive(lease_id as i64).await?;


    debug_println!(BLUE, "[KEEP_ALIVE]", RESET, "Starting keep-alive loop lease_id={}, ttl={}, deadline={:?}", 
        lease_id, ttl, deadline);

    loop {
        debug_println!(GREEN, "[KEEP_ALIVE]", RESET, "Loop iteration lease_id={}, ttl={}, deadline={:?}", 
                 lease_id, ttl, deadline);
        // if the deadline is exceeded, then we have failed to issue a heartbeat in time
        // we may be permanently disconnected from the etcd server, so we are now officially done
        if deadline < std::time::Instant::now() {
            debug_println!(RED, "[KEEP_ALIVE]", RESET, "Deadline exceeded lease_id={}", lease_id);
            return Err(error!(
                "Unable to refresh lease - deadline exceeded. Check etcd server status"
            ));
        }

        tokio::select! {
            biased;

            status = heartbeat_receiver.message() => {
                match status {
                    Ok(Some(resp)) => {
                        // Good response - process the heartbeat
                        debug_println!(GREEN, "[KEEP_ALIVE]", RESET, "❤️ Heartbeat response received lease_id={}", lease_id);
                        tracing::trace!(lease_id, "keep alive response received: {:?}", resp);

                        // update ttl and deadline
                        ttl = resp.ttl() as u64;
                        deadline = create_deadline(ttl)?;

                        if resp.ttl() == 0 {
                            return Err(error!("Unable to maintain lease - expired or revoked. Check etcd server status"));
                        }
                    },
                    Ok(None) => {
                        // No response received - this is expected in some cases
                        debug_println!(YELLOW, "[KEEP_ALIVE]", RESET, "⁇ No response received for lease_id={}", lease_id);
                    },
                    Err(e) => {
                        // Error getting the message
                        debug_println!(RED, "[KEEP_ALIVE]", RESET, "❌ Error receiving heartbeat message for lease_id={}: {}", lease_id, e);
                        return Err(e.into());
                    }
                }
            }

            _ = token.cancelled() => {
                debug_println!(RED, "[KEEP_ALIVE]", RESET, "Cancellation token triggered lease_id={}", lease_id);
                tracing::trace!(lease_id, "cancellation token triggered; revoking lease");
                let _ = client.revoke(lease_id as i64).await?;
                return Ok(());
            }

            _ = tokio::time::sleep(tokio::time::Duration::from_secs(ttl / 2)) => {
                tracing::trace!(lease_id, "sending keep alive");
                debug_println!(GREEN, "[KEEP_ALIVE]", RESET, "Slept for {:?} seconds lease_id={}, sending heartbeat ❤️", ttl / 2, lease_id);

                // if we get a error issuing the heartbeat, set the ttl to 0
                // this will allow us to poll the response stream once and the cancellation token once, then
                // immediately try to tick the heartbeat
                // this will repeat until either the heartbeat is reestablished or the deadline is exceeded
                if let Err(e) = heartbeat_sender.keep_alive().await {
                    debug_println!(RED, "[KEEP_ALIVE]", RED, "Error with lease_id={}: {}", lease_id, e);
                    tracing::warn!(
                        lease_id,
                        error = %e,
                        "Unable to send lease heartbeat. Check etcd server status"
                    );
                    ttl = 0;
                }
            }

        }
    }
}

/// Create a deadline for a given time-to-live (TTL).
fn create_deadline(ttl: u64) -> Result<std::time::Instant> {
    Ok(std::time::Instant::now() + std::time::Duration::from_secs(ttl))
}
