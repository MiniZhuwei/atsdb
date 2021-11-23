#![allow(clippy::needless_return)]

mod proto {
    include!("../src/server/proto/atsdb.proto.rs");
}

use crate::proto::scalar::Data;
use crate::proto::{InsertRequest, Label, Request, Scalar};
use proto::service_client::ServiceClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ServiceClient::connect("http://[::1]:1107").await?;
    let request = tonic::Request::new(InsertRequest {
        requests: vec![Request {
            table_name: "default".to_string(),
            timestamp: chrono::Utc::now().timestamp_nanos(),
            labels: vec![Label {
                key: "test".to_string(),
                value: "test".to_string(),
            }],
            scalars: vec![Scalar {
                key: "value".to_string(),
                data: Some(Data::Int(1)),
            }],
        }],
    });
    let response = client.insert(request).await?;
    println!("RESPONSE={:?}", response);
    return Ok(());
}
