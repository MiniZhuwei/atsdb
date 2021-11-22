pub mod proto;

use crate::server::proto::scalar::Data;
use crate::server::proto::{
    InsertRequest, InsertResponse, Response as ServiceResponse, Status as ServiceStatus,
};
use crate::storage::chunk::Scalar;
use crate::storage::db::DB;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use proto::service_server::Service;
use std::time::Duration;
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct Server {
    db: DB,
}

impl Server {
    pub fn new(query_worker_num: usize, write_worker_num: usize) -> Self {
        return Self {
            db: DB::new(
                query_worker_num,
                write_worker_num,
                1440,
                5 * Duration::SECOND,
                24,
            ),
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
                .insert(&request.table_name, timestamp, labels, scalars)
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
