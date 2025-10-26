use dynamo_runtime::transports::etcd::{Client, ClientOptions};
use dynamo_runtime::Runtime;
use tokio::time::{sleep, Duration};

fn main() -> anyhow::Result<()> {
    // Initialize Dynamo runtime
    let runtime = Runtime::from_settings()?;
    
    // Run the async code in the Dynamo runtime's primary executor
    runtime.primary().block_on(async {
        // Create etcd client configuration
        let endpoints: Vec<String> = std::env::var("ETCD_ENDPOINTS")?
            .split(',')
            .map(|s| s.to_string())
            .collect();
        
        let client_options = ClientOptions {
            etcd_url: endpoints,
            etcd_connect_options: None,
            attach_lease: true,
        };
        
        // Create the Dynamo etcd client
        let client = Client::new(client_options, runtime.clone()).await?;
        
        println!("Connected to etcd with primary lease ID: {}", client.lease_id());
        
        // Test basic KV operations with lease
        let key = "test/lease_key";
        let value = b"test_value_with_lease";
        
        // Put a key with the primary lease
        client.kv_put(key, value, None).await?;
        println!("Put key '{}' with primary lease", key);
        
        // Create a new lease for testing
        let test_lease = client.create_lease(15).await?;
        println!("Created test lease with ID: {}", test_lease.id());
        
        // Put another key with the test lease
        let test_key = "test/lease_key_2";
        let test_value = b"test_value_with_test_lease";
        client.kv_put(test_key, test_value, Some(test_lease.id())).await?;
        println!("Put key '{}' with test lease ID: {}", test_key, test_lease.id());
        
        // Test lease validation
        let is_valid = test_lease.is_valid().await?;
        println!("Test lease is valid: {}", is_valid);
        
        // Test child token
        let _child_token = test_lease.child_token();
        println!("Created child token from test lease");
        
        // Test primary lease
        let primary_lease = client.primary_lease();
        println!("Primary lease ID: {}", primary_lease.id());
        
        // Test lease revocation
        println!("Revoking test lease...");
        test_lease.revoke();
        
        let is_valid_after_revoke = test_lease.is_valid().await?;
        println!("Test lease is valid after revocation: {}", is_valid_after_revoke);
        
        // Keep the primary lease alive by running the runtime
        println!("Keeping primary lease alive. Press Ctrl+C to stop...");
        loop { 
            sleep(Duration::from_secs(5)).await;
            let primary_valid = primary_lease.is_valid().await?;
            println!("Primary lease still valid: {}", primary_valid);
        }
    })
}
