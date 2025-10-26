use dynamo_runtime::transports::etcd::{Client, ClientOptions};
use dynamo_runtime::Runtime;
use tokio::time::{sleep, Duration};

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
        
        println!("Connected to etcd with primary lease ID: {}", client.lease_id());
        
        // Get the primary lease
        let primary_lease = client.primary_lease();
        println!("Primary lease ID: {}", primary_lease.id());
        
        // Keep the primary lease alive by running the runtime
        println!("Monitoring primary lease. Press Ctrl+C to stop...");
        println!("Try running 'make restart-leader' in another terminal to test leader re-election!");
        
        loop { 
            sleep(Duration::from_secs(5)).await;
            let primary_valid = primary_lease.is_valid().await
                .map_err(|e| anyhow::anyhow!("Failed to check primary lease validity: {}", e))?;
            println!("Primary lease still valid: {}", primary_valid);
            
            // If lease becomes invalid, it means there was likely a leader re-election
            if !primary_valid {
                println!("⚠️  PRIMARY LEASE BECAME INVALID!");
                println!("This likely indicates a leader re-election or network partition occurred.");
                println!("The Dynamo runtime should handle this by shutting down gracefully.");
                
                // In a real Dynamo application, this would trigger runtime shutdown
                // For testing purposes, we'll just exit
                println!("Exiting due to lease invalidation...");
                break;
            }
        }
        
        Ok::<(), anyhow::Error>(())
    })
}
