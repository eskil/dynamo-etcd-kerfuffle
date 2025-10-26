use dynamo_runtime::transports::etcd::{Client, ClientOptions};
use dynamo_runtime::Runtime;
use tokio::time::{sleep, Duration};

// Import the debug macro
use dynamo_runtime::debug_println;

fn main() -> anyhow::Result<()> {
    // Initialize Dynamo runtime
    let runtime = Runtime::from_settings()?;
    
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
        
        // Keep the primary lease alive by running the runtime
        debug_println!(WHITE, "[MAIN]", RESET, "Monitoring primary lease. Press Ctrl+C to stop...");
        debug_println!(WHITE, "[MAIN]", RESET, "Try running 'make restart-leader' in another terminal to test leader re-election!");
        
        loop { 
            sleep(Duration::from_secs(5)).await;
            let primary_valid = primary_lease.is_valid().await
                .map_err(|e| anyhow::anyhow!("Failed to check primary lease validity: {}", e))?;
            if primary_valid {
                debug_println!(WHITE, "[MAIN]", RESET, "Primary lease still valid: {}", primary_valid);
                debug_println!(WHITE, "", RESET, "---");
                debug_println!(WHITE, "", RESET, "");
            } else {        
                debug_println!(WHITE, "[MAIN]", RED, "⚠️  PRIMARY LEASE BECAME INVALID!");
                debug_println!(WHITE, "[MAIN]", RED, "Exiting due to lease invalidation...");
                break;
            }
        }
        
        Ok::<(), anyhow::Error>(())
    })
}
