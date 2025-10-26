// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::time::Duration;

// ANSI color codes
const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const RED: &str = "\x1b[31m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const BLUE: &str = "\x1b[34m";
const MAGENTA: &str = "\x1b[35m";
const CYAN: &str = "\x1b[36m";
const WHITE: &str = "\x1b[37m";

/// Create a [`Lease`] with a given time-to-live (TTL) attached to the [`CancellationToken`].
pub async fn create_lease(
    mut lease_client: LeaseClient,
    ttl: u64,
    token: CancellationToken,
) -> Result<Lease> {
    eprintln!("{}{}[CREATE_LEASE]{} Creating lease with ttl={}", CYAN, BOLD, RESET, ttl);
    
    let lease = lease_client.grant(ttl as i64, None).await?;
    eprintln!("{}{}[CREATE_LEASE]{} Lease granted with id={}, ttl={}", GREEN, BOLD, RESET, lease.id(), lease.ttl());

    let id = lease.id() as u64;
    let ttl = lease.ttl() as u64;
    let child = token.child_token();
    let clone = token.clone();

    eprintln!("{}{}[CREATE_LEASE]{} Spawning keep-alive task for lease_id={}", BLUE, BOLD, RESET, id);
    tokio::spawn(async move {
        eprintln!("{}{}[CREATE_LEASE]{} Keep-alive task started for lease_id={}", GREEN, BOLD, RESET, id);
        
        // Add a panic hook to catch any panics
        std::panic::set_hook(Box::new(move |panic_info| {
            eprintln!("{}{}[CREATE_LEASE] PANIC{} in keep-alive task for lease_id={}: {:?}", RED, BOLD, RESET, id, panic_info);
        }));
        
        // Feature flag to enable/disable retry logic
        const ENABLE_RETRY: bool = true;
        
        if ENABLE_RETRY {
            // Retry logic for keep-alive failures
            let mut retry_count = 0;
            const MAX_RETRIES: u32 = 5;
            const RETRY_DELAY: Duration = Duration::from_secs(1);
            
            eprintln!("{}{}[CREATE_LEASE]{} Using retry logic for lease_id={}", YELLOW, BOLD, RESET, id);
            
            loop {
                match keep_alive(lease_client.clone(), id, ttl, child.clone()).await {
                    Ok(_) => {
                        eprintln!("{}{}[CREATE_LEASE]{} Keep-alive task exited successfully for lease_id={}", GREEN, BOLD, RESET, id);
                        tracing::trace!("keep alive task exited successfully");
                        break;
                    },
                    Err(e) => {
                        retry_count += 1;
                        eprintln!("{}{}[CREATE_LEASE]{} Keep-alive task failed for lease_id={} (attempt {}/{}): {}", 
                                 RED, BOLD, RESET, id, retry_count, MAX_RETRIES, e);
                        tracing::error!(
                            error = %e,
                            "Unable to maintain lease. Check etcd server status (attempt {}/{})",
                            retry_count, MAX_RETRIES
                        );
            
                        
                        if retry_count >= MAX_RETRIES {
                            eprintln!("{}{}[CREATE_LEASE]{} Max retries exceeded for lease_id={}, giving up", RED, BOLD, RESET, id);
                            tracing::error!(
                                error = %e,
                                "Unable to maintain lease after {} retries. Check etcd server status",
                                MAX_RETRIES
                            );
                            token.cancel();
                            break;
                        }
                        
                        eprintln!("{}{}[CREATE_LEASE]{} Retrying keep-alive for lease_id={} in {:?}", YELLOW, BOLD, RESET, id, RETRY_DELAY);
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                }
            }
        } else {
            // Original logic without retry
            eprintln!("[CREATE_LEASE] Using original logic (no retry) for lease_id={}", id);
            
            match keep_alive(lease_client, id, ttl, child).await {
                Ok(_) => {
                    eprintln!("[CREATE_LEASE] Keep-alive task exited successfully for lease_id={}", id);
                    tracing::trace!("keep alive task exited successfully");
                },
                Err(e) => {
                    eprintln!("[CREATE_LEASE] Keep-alive task failed for lease_id={}: {}", id, e);
                    tracing::error!(
                        error = %e,
                        "Unable to maintain lease. Check etcd server status"
                    );
                    token.cancel();
                }
            }
        }
        
        eprintln!("[CREATE_LEASE] Keep-alive task completely finished for lease_id={}", id);
    });

    eprintln!("[CREATE_LEASE] Returning lease with id={}", id);
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
    eprintln!("{}{}[KEEP_ALIVE]{} Starting keep-alive for lease_id={}, initial_ttl={}", MAGENTA, BOLD, RESET, lease_id, ttl);
    
    let mut ttl = ttl;
    let mut deadline = create_deadline(ttl)?;
    eprintln!("{}{}[KEEP_ALIVE]{} Initial deadline: {:?}", CYAN, BOLD, RESET, deadline);

    let mut client = client;
    eprintln!("{}{}[KEEP_ALIVE]{} Attempting to create keep-alive stream for lease_id={}", BLUE, BOLD, RESET, lease_id);
    let (mut heartbeat_sender, mut heartbeat_receiver) = match client.keep_alive(lease_id as i64).await {
        Ok(stream) => {
            eprintln!("{}{}[KEEP_ALIVE]{} Keep-alive stream established successfully for lease_id={}", GREEN, BOLD, RESET, lease_id);
            stream
        },
        Err(e) => {
            eprintln!("{}{}[KEEP_ALIVE] FAILED{} to create keep-alive stream for lease_id={}: {}", RED, BOLD, RESET, lease_id, e);
            return Err(e.into());
        }
    };

    let mut heartbeat_count = 0;
    let mut response_count = 0;

    loop {
        // if the deadline is exceeded, then we have failed to issue a heartbeat in time
        // we may be permanently disconnected from the etcd server, so we are now officially done
        if deadline < std::time::Instant::now() {
            eprintln!("[KEEP_ALIVE] DEADLINE EXCEEDED for lease_id={}, deadline={:?}, now={:?}", 
                     lease_id, deadline, std::time::Instant::now());
            return Err(error!(
                "Unable to refresh lease - deadline exceeded. Check etcd server status"
            ));
        }

        let time_until_deadline = deadline.duration_since(std::time::Instant::now());
        eprintln!("[KEEP_ALIVE] Loop iteration for lease_id={}, ttl={}, time_until_deadline={:?}", 
                 lease_id, ttl, time_until_deadline);

        tokio::select! {
            biased;

            status = heartbeat_receiver.message() => {
                response_count += 1;
                if let Some(resp) = status? {
                    eprintln!("[KEEP_ALIVE] Response #{} received for lease_id={}: ttl={}", 
                             response_count, lease_id, resp.ttl());

                    // update ttl and deadline
                    let old_ttl = ttl;
                    ttl = resp.ttl() as u64;
                    deadline = create_deadline(ttl)?;
                    eprintln!("[KEEP_ALIVE] Updated lease_id={}: ttl {} -> {}, new_deadline={:?}", 
                             lease_id, old_ttl, ttl, deadline);

                    if resp.ttl() == 0 {
                        eprintln!("[KEEP_ALIVE] LEASE EXPIRED for lease_id={}", lease_id);
                        return Err(error!("Unable to maintain lease - expired or revoked. Check etcd server status"));
                    }

                } else {
                    eprintln!("[KEEP_ALIVE] No response received for lease_id={}", lease_id);
                }
            }

            _ = token.cancelled() => {
                eprintln!("[KEEP_ALIVE] CANCELLATION TOKEN TRIGGERED for lease_id={}", lease_id);
                tracing::trace!(lease_id, "cancellation token triggered; revoking lease");
                let _ = client.revoke(lease_id as i64).await?;
                eprintln!("[KEEP_ALIVE] Lease revoked and exiting for lease_id={}", lease_id);
                return Ok(());
            }

            _ = tokio::time::sleep(tokio::time::Duration::from_secs(ttl / 2)) => {
                heartbeat_count += 1;
                eprintln!("[KEEP_ALIVE] Sending heartbeat #{} for lease_id={} (ttl={})", 
                         heartbeat_count, lease_id, ttl);

                // if we get a error issuing the heartbeat, set the ttl to 0
                // this will allow us to poll the response stream once and the cancellation token once, then
                // immediately try to tick the heartbeat
                // this will repeat until either the heartbeat is reestablished or the deadline is exceeded
                if let Err(e) = heartbeat_sender.keep_alive().await {
                    eprintln!("[KEEP_ALIVE] HEARTBEAT FAILED for lease_id={}: {}", lease_id, e);
                    tracing::warn!(
                        lease_id,
                        error = %e,
                        "Unable to send lease heartbeat. Check etcd server status"
                    );
                    ttl = 0;
                    eprintln!("[KEEP_ALIVE] Set ttl=0 for immediate retry on lease_id={}", lease_id);
                } else {
                    eprintln!("[KEEP_ALIVE] Heartbeat #{} sent successfully for lease_id={}", 
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
