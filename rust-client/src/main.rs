use dynamo_runtime::transports::etcd::{Client, ClientOptions};
use dynamo_runtime::Runtime;
use std::time::Instant;
use tokio::time::{sleep, Duration};

// Import the debug macro
use dynamo_runtime::debug_println;

/// Format elapsed time in a human-friendly way
fn format_elapsed(elapsed: std::time::Duration) -> String {
    let total_secs = elapsed.as_secs();
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    
    match (hours, mins, secs) {
        (0, 0, s) => format!("{}s", s),
        (0, m, s) => format!("{}min {}s", m, s),
        (h, m, s) => format!("{}hr {}min {}s", h, m, s),
    }
}

fn main() -> anyhow::Result<()> {
    // Initialize Dynamo runtime
    let runtime = Runtime::from_settings()?;
    
    // Record start time
    let start_time = Instant::now();
    
    // Run the async code in the Dynamo runtime's primary executor
    runtime.primary().block_on(async {
        // Create etcd client configuration
        let endpoints: Vec<String> = std::env::var("ETCD_ENDPOINTS")
            .map_err(|e| anyhow::anyhow!("Failed to get ETCD_ENDPOINTS: {}", e))?
            .split(',')
            .map(|s| s.to_string())
            .collect();
        
        let client_options = ClientOptions {
            etcd_url: endpoints,
            etcd_connect_options: None,
            attach_lease: true,
        };
        
        // Create the Dynamo etcd client
        let client = Client::new(client_options, runtime.clone()).await
            .map_err(|e| anyhow::anyhow!("Failed to create etcd client: {}", e))?;
        
        debug_println!(WHITE, "[MAIN]", RESET, "Connected to etcd with primary lease ID: {}", client.lease_id());
        
        // Get the primary lease
        let primary_lease = client.primary_lease();
        debug_println!(WHITE, "[MAIN]", RESET, "Primary lease ID: {}", primary_lease.id());
        // Create a secondary lease with a 15 second TTL
        let secondary_lease = client.create_lease(10).await
            .map_err(|e| anyhow::anyhow!("Failed to create secondary lease: {}", e))?;
        debug_println!(WHITE, "[MAIN]", RESET, "Secondary lease ID: {}", secondary_lease.id());
        
        // Keep the primary lease alive by running the runtime
        debug_println!(WHITE, "[MAIN]", RESET, "Monitoring primary lease. Press Ctrl+C to stop...");
        debug_println!(WHITE, "[MAIN]", WHITE, "ℹ️ Try running 'make restart-leader' in another terminal to test leader re-election");
        
        loop { 
            sleep(Duration::from_secs(5)).await;
            let primary_valid: bool = primary_lease.is_valid().await
                .map_err(|e| anyhow::anyhow!("Failed to check primary lease validity: {}", e))?;
            let secondary_valid: bool = secondary_lease.is_valid().await
                .map_err(|e| anyhow::anyhow!("Failed to check secondary lease validity: {}", e))?;
            let elapsed = start_time.elapsed();
            let elapsed_str = format_elapsed(elapsed);
            if primary_valid && secondary_valid {
                debug_println!(WHITE, "[MAIN]", RESET, "Primary lease valid: {} Secondary lease valid: {} (elapsed: {})", primary_valid, secondary_valid, elapsed_str);
                debug_println!(WHITE, "", RESET, "---");
                debug_println!(WHITE, "", RESET, "");
            } else {        
                if !primary_valid {
                    debug_println!(WHITE, "[MAIN]", RED, "⚠️  PRIMARY LEASE BECAME INVALID! (elapsed: {})", elapsed_str);
                }
                if !secondary_valid {
                    debug_println!(WHITE, "[MAIN]", RED, "⚠️  SECONDARY LEASE BECAME INVALID! (elapsed: {})", elapsed_str);
                }
                debug_println!(WHITE, "[MAIN]", RED, "Exiting due to lease invalidation...");
                break;
            }
        }
        
        Ok::<(), anyhow::Error>(())
    })
}
