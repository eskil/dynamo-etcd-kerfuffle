use etcd_client::{Client, LeaseKeeper};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoints: Vec<String> = std::env::var("ETCD_ENDPOINTS")?
        .split(',')
        .map(|s| s.to_string())
        .collect();

    let mut client = Client::connect(endpoints, None).await?;

    let lease = client.lease_grant(10, None).await?;
    let lease_id = lease.id();

    client.kv_client().put("foo", "bar", Some(lease_id)).await?;
    println!("Put key with lease ID: {}", lease_id);

    let mut keeper = client.lease_keep_alive(lease_id).await?;
    tokio::spawn(async move {
        while let Some(resp) = keeper.message().await.unwrap() {
            println!("Lease renewed, TTL: {}", resp.ttl());
        }
    });

    loop { sleep(Duration::from_secs(5)).await; }
}
