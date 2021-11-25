pub mod proto;

use crate::server::proto::scalar::Data;
use crate::server::proto::{
    InsertRequest, InsertResponse, Response as ServiceResponse, Status as ServiceStatus,
};
use crate::storage::chunk::Scalar;
use crate::storage::db::{ShardedDB, DB};
use crate::storage::util::thread::CoreBoundWorkers;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use proto::service_server::Service;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct Server {
    db: ShardedDB,
}

impl Server {
    pub async fn new(worker_num: usize) -> Self {
        let workers = CoreBoundWorkers::new(worker_num).unwrap();
        return Self {
            db: ShardedDB::new(&workers, 1440, 5 * Duration::SECOND, 24).await,
        };
    }
}

#[async_trait]
impl Service for Server {
    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        let mut results = Vec::new();
        for request in request.into_inner().requests {
            let labels = request
                .labels
                .iter()
                .map(|label| (label.key.clone(), label.value.clone()))
                .collect();
            let scalars = request
                .scalars
                .iter()
                .map(|scalar| {
                    (
                        scalar.key.clone(),
                        match scalar.data.clone().unwrap() {
                            Data::Int(i) => Scalar::Int(i),
                            Data::Float(f) => Scalar::Float(f),
                        },
                    )
                })
                .collect();
            let timestamp = Utc.timestamp_nanos(request.timestamp).into();
            let result = match self
                .db
                .insert(
                    Arc::from(request.table_name.as_ref()),
                    timestamp,
                    labels,
                    scalars,
                )
                .await
            {
                Ok(_) => ServiceResponse {
                    status: ServiceStatus::Ok as i32,
                    description: None,
                },
                Err(err) => ServiceResponse {
                    status: ServiceStatus::InternalError as i32,
                    description: Some(err.to_string()),
                },
            };
            results.push(result);
        }
        return Ok(Response::new(InsertResponse { responses: results }));
    }
}
