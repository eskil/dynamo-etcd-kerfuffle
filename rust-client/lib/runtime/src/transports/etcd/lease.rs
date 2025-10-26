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
        
        // Feature flag to enable/disable retry logic
        const ENABLE_RETRY: bool = true;
        
        if ENABLE_RETRY {
            // Retry logic for keep-alive failures
            let mut retry_count = 0;
            const MAX_RETRIES: u32 = 20;
            const RETRY_DELAY: Duration = Duration::from_millis(500);
            
            debug_println!(MAGENTA, "[CREATE_LEASE]", RESET, "Using retry logic lease_id={}", id);
            
            loop {
                match keep_alive(lease_client.clone(), id, ttl, child.clone()).await {
                    Ok(_) => {
                        debug_println!(YELLOW, "[CREATE_LEASE]", RESET, "Keep-alive task EXITED lease_id={}, retry_count={}", id, retry_count);
                        tracing::trace!("keep alive task exited");                        
                        retry_count = 0;
                        continue;
                    },
                    Err(e) => {
                        retry_count += 1;
                        debug_println!(RED, "[CREATE_LEASE]", YELLOW, "Keep-alive task failed lease_id={} (attempt {}/{}): {}", id, retry_count, MAX_RETRIES, e);
                        tracing::error!(
                            error = %e,
                            "Unable to maintain lease. Check etcd server status (attempt {}/{})",
                            retry_count, MAX_RETRIES
                        );
            
                        
                        if retry_count >= MAX_RETRIES {
                            debug_println!(RED, "[CREATE_LEASE]", RED, "Max retries {} exceeded lease_id={}, giving up", MAX_RETRIES, id);
                            tracing::error!(
                                error = %e,
                                "Unable to maintain lease after {} retries. Check etcd server status",
                                MAX_RETRIES
                            );
                            token.cancel();
                            break;
                        }
                        
                        debug_println!(YELLOW, "[CREATE_LEASE]", YELLOW, "Retrying keep-alive lease_id={} in {:?}", id, RETRY_DELAY);
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                }
            }
        } else {
            // Original logic without retry
            debug_println!(MAGENTA, "[CREATE_LEASE]", RESET, "Using original logic (no retry) lease_id={}", id);
            
            match keep_alive(lease_client, id, ttl, child).await {
                Ok(_) => {
                    debug_println!(BLUE, "[CREATE_LEASE]", RESET, "Keep-alive task EXITED successfully lease_id={}", id);
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
/// If
pub async fn keep_alive(
    client: LeaseClient,
    lease_id: u64,
    ttl: u64,
    token: CancellationToken,
) -> Result<()> {
    debug_println!(GREEN, "[KEEP_ALIVE]", RESET, "Starting keep-alive lease_id={}, initial_ttl={}", lease_id, ttl);
    
    let mut ttl = ttl;
    let mut deadline = create_deadline(ttl)?;
    debug_println!(GREEN, "[KEEP_ALIVE]", RESET, "Initial deadline: {:?}", deadline);

    let mut client = client;
    debug_println!(GREEN, "[KEEP_ALIVE]", RESET, "Attempting to create keep-alive stream lease_id={}", lease_id);
    let (mut heartbeat_sender, mut heartbeat_receiver) = match client.keep_alive(lease_id as i64).await {
        Ok(stream) => {
            debug_println!(GREEN, "[KEEP_ALIVE]", RESET, "Keep-alive stream established successfully lease_id={}", lease_id);
            stream
        },
        Err(e) => {
            debug_println!(RED, "[KEEP_ALIVE]", RESET, "FAILED to create keep-alive stream lease_id={}: {}", lease_id, e);
            return Err(e.into());
        }
    };

    let mut heartbeat_count = 0;
    let mut response_count = 0;

    loop {
        // if the deadline is exceeded, then we have failed to issue a heartbeat in time
        // we may be permanently disconnected from the etcd server, so we are now officially done
        if deadline < std::time::Instant::now() {
            debug_println!(RED, "[KEEP_ALIVE]", RESET, "DEADLINE EXCEEDED lease_id={}, deadline={:?}, now={:?}", 
                     lease_id, deadline, std::time::Instant::now());
            return Err(error!(
                "Unable to refresh lease - deadline exceeded. Check etcd server status"
            ));
        }

        let time_until_deadline = deadline.duration_since(std::time::Instant::now());
        debug_println!(GREEN, "[KEEP_ALIVE]", RESET, "Loop iteration lease_id={}, ttl={}, time_until_deadline={:?}", 
                 lease_id, ttl, time_until_deadline);

        tokio::select! {
            biased;

            status = heartbeat_receiver.message() => {
                response_count += 1;
                if let Some(resp) = status? {
                    debug_println!(GREEN, "[KEEP_ALIVE]", RESET, "Response #{} received lease_id={}: ttl={}", 
                             response_count, lease_id, resp.ttl());

                    // update ttl and deadline
                    let old_ttl = ttl;
                    ttl = resp.ttl() as u64;
                    deadline = create_deadline(ttl)?;
                    debug_println!(GREEN, "[KEEP_ALIVE]", RESET, "Updated lease_id={}: ttl {} -> {} new_deadline={:?}", 
                             lease_id, old_ttl, ttl, deadline);

                    if resp.ttl() == 0 {
                        debug_println!(RED, "[KEEP_ALIVE]", RED, "LEASE EXPIRED lease_id={}", lease_id);
                        return Err(error!("Unable to maintain lease - expired or revoked. Check etcd server status"));
                    }

                } else {
                    debug_println!(YELLOW, "[KEEP_ALIVE]", RED, "No response received lease_id={}", lease_id);
                }
            }

            _ = token.cancelled() => {
                debug_println!(RED, "[KEEP_ALIVE]", RESET, "CANCELLATION TOKEN TRIGGERED lease_id={}", lease_id);
                tracing::trace!(lease_id, "cancellation token triggered; revoking lease");
                let _ = client.revoke(lease_id as i64).await?;
                debug_println!(GREEN, "[KEEP_ALIVE]", RESET, "Lease revoked and exiting lease_id={}", lease_id);
                return Ok(());
            }

            _ = tokio::time::sleep(tokio::time::Duration::from_secs(ttl / 2)) => {
                heartbeat_count += 1;
                debug_println!(GREEN, "[KEEP_ALIVE]", RESET, "Sending heartbeat #{} lease_id={} (ttl={})", 
                         heartbeat_count, lease_id, ttl);

                // if we get a error issuing the heartbeat, set the ttl to 0
                // this will allow us to poll the response stream once and the cancellation token once, then
                // immediately try to tick the heartbeat
                // this will repeat until either the heartbeat is reestablished or the deadline is exceeded
                if let Err(e) = heartbeat_sender.keep_alive().await {
                    debug_println!(RED, "[KEEP_ALIVE]", RESET, "HEARTBEAT FAILED lease_id={}: {}", lease_id, e);
                    tracing::warn!(
                        lease_id,
                        error = %e,
                        "Unable to send lease heartbeat. Check etcd server status"
                    );
                    ttl = 0;
                    debug_println!(YELLOW, "[KEEP_ALIVE]", RESET, "Set ttl=0 for immediate retry lease_id={}", lease_id);
                } else {
                    debug_println!(GREEN, "[KEEP_ALIVE]", RESET, "Heartbeat #{} sent successfully lease_id={}", 
                             heartbeat_count, lease_id);
                }
            }

        }
    }
}

/// Create a deadline for a given time-to-live (TTL).
fn create_deadline(ttl: u64) -> Result<std::time::Instant> {
    Ok(std::time::Instant::now() + std::time::Duration::from_secs(ttl))
}
