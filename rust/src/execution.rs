#![feature(generators)]
#![feature(type_alias_impl_trait)]


use crate::error::Result;
use arrow::record_batch::RecordBatch;

use async_stream::stream;

use futures::pin_mut;
use futures::stream::Stream;
use futures::stream::StreamExt;

#[tokio::main]
async fn main(){

    let data = DataExec { batches: vec![] };
    let projection = ProjectionExec { input: data };

    let stream = projection.execute().unwrap();

    pin_mut!(stream); // needed for iteration

    while let Some(value) = stream.next().await {
        println!("got {}", value);
    }
}

pub type RecordBatches = impl Stream<Item=RecordBatch>;

pub trait Exec {
    fn execute(&self) -> Result<RecordBatches>;
}

pub struct DataExec {
    pub batches: Vec<RecordBatch>
}

impl Exec for DataExec {
    fn execute(&self) -> Result<RecordBatches> {
        stream! {
            self.batches.iter().for_each(|batch| yield batch);
        }
    }
}

pub struct ProjectionExec {
    pub input: Exec
}

impl Exec for ProjectionExec {
    fn execute(&self) -> Result<RecordBatches> {
        stream! {
            while let Some(batch) = self.input.next().await? {
                //TODO apply projection
                yield batch;
            }
        }
    }
}