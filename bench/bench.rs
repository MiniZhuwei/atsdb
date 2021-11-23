#![allow(clippy::needless_return)]

mod proto {
    include!("../src/server/proto/atsdb.proto.rs");
}

use crate::proto::scalar::Data;
use crate::proto::{InsertRequest, Label, Request, Scalar};
use proto::service_client::ServiceClient;
use rand::rngs::StdRng;
use rand::{distributions::Alphanumeric, Rng};
use std::time::SystemTime;

#[tokio::main(flavor = "multi_thread", worker_threads = 12)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let label_value = generate_string(8, 20);
    let label_name = generate_string(8, 10);
    let mut tasks = Vec::new();
    let round = 20000;
    let parallel = 12;
    let start = SystemTime::now();
    for _ in 0..parallel {
        let lv_clone = label_value.clone();
        let ln_clone = label_name.clone();
        let join = tokio::spawn(async move {
            let mut client = ServiceClient::connect("http://[::1]:1107").await.unwrap();
            let mut rng: StdRng = rand::SeedableRng::from_entropy();
            for _ in 0..round {
                let request = tonic::Request::new(InsertRequest {
                    requests: vec![Request {
                        table_name: "default".to_string(),
                        timestamp: chrono::Utc::now().timestamp_nanos(),
                        labels: ln_clone
                            .iter()
                            .map(|name| Label {
                                key: name.clone(),
                                value: lv_clone
                                    .get(rng.gen_range(0..lv_clone.len()))
                                    .unwrap()
                                    .to_string(),
                            })
                            .collect(),
                        scalars: vec![Scalar {
                            key: "value".to_string(),
                            data: Some(Data::Int(1)),
                        }],
                    }],
                });
                client.insert(request).await.unwrap();
            }
        });
        tasks.push(join);
    }
    for task in tasks.iter_mut() {
        task.await.unwrap();
    }
    println!(
        "{:?}",
        round as f64 * parallel as f64
            / SystemTime::now()
                .duration_since(start)
                .unwrap()
                .as_secs_f64()
    );
    return Ok(());
}

fn generate_string(size: usize, len: usize) -> Vec<String> {
    return (0..size)
        .map(|_| {
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(len)
                .map(char::from)
                .collect()
        })
        .collect();
}
