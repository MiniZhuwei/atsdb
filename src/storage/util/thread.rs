use snafu::Snafu;
use std::future::Future;
use std::pin::Pin;
use std::thread;
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::task::{spawn_local, LocalSet};

#[derive(Snafu, Debug)]
pub enum CoreBoundThreadsError {
    #[snafu(display(
        "worker num can not be greater than cpu num, require: {}, has: {}",
        require,
        has
    ))]
    NoMuchCore { require: usize, has: usize },
}

pub type LocalTask = Pin<Box<dyn 'static + Future<Output = ()>>>;

pub type Task = Box<dyn FnOnce() -> LocalTask + Send>;

#[derive(Debug)]
pub struct Worker {
    sender: Sender<Task>,
}

impl Worker {
    pub async fn spawn<G: FnOnce() -> LocalTask + Send + 'static>(
        &self,
        future: G,
    ) -> Result<(), SendError<Task>> {
        return self.sender.send(Box::new(future)).await;
    }
}

#[derive(Debug)]
pub struct CoreBoundWorkers {
    senders: Vec<Worker>,
    shutdown: Receiver<()>,
}

impl CoreBoundWorkers {
    pub fn new(worker_num: usize) -> Result<Self, CoreBoundThreadsError> {
        let mut core_ids = core_affinity::get_core_ids().unwrap();
        if core_ids.len() < worker_num {
            return Err(CoreBoundThreadsError::NoMuchCore {
                require: core_ids.len(),
                has: worker_num,
            });
        }

        let mut senders = Vec::new();
        let (shutdown, shutdown_recv) = mpsc::channel::<()>(1);
        for id in core_ids.drain(0..worker_num) {
            let shutdown = shutdown.clone();
            let (send, mut recv) = mpsc::channel::<Task>(1);
            thread::spawn(move || {
                core_affinity::set_for_current(id);
                let local = LocalSet::new();
                let runtime = Builder::new_current_thread().enable_all().build().unwrap();
                local.spawn_local(async move {
                    let (task_shutdown, mut task_shutdown_recv) = mpsc::channel::<()>(1);
                    while let Some(task) = recv.recv().await {
                        let task_shutdown = task_shutdown.clone();
                        spawn_local(async move {
                            task().await;
                            drop(task_shutdown);
                        });
                    }
                    drop(task_shutdown);
                    task_shutdown_recv.recv().await;
                });
                runtime.block_on(local);
                drop(shutdown);
            });
            senders.push(Worker { sender: send });
        }
        return Ok(Self {
            senders,
            shutdown: shutdown_recv,
        });
    }

    pub async fn spawn<G: FnOnce() -> LocalTask + Send + 'static>(&self, id: usize, future: G) {
        if self.senders.get(id).unwrap().spawn(future).await.is_err() {
            panic!("future error")
        };
    }

    pub fn iter(&self) -> impl Iterator<Item = &Worker> {
        return self.senders.iter();
    }

    pub async fn close(mut self) {
        drop(self.senders);
        let _ = self.shutdown.recv().await;
    }
}

#[cfg(test)]
mod tests {
    use super::CoreBoundWorkers;
    use std::borrow::Borrow;
    use std::rc::Rc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_worker() {
        let worker = CoreBoundWorkers::new(12).unwrap();
        worker
            .spawn(0, move || {
                Box::pin(async move {
                    let _ = Rc::new("ref test");
                    println!("start 0");
                    tokio::time::sleep(Duration::SECOND).await;
                    println!("end 0");
                })
            })
            .await;
        worker
            .spawn(0, move || {
                Box::pin(async move {
                    let _ = Rc::new("ref test");
                    println!("start 1");
                    tokio::time::sleep(Duration::SECOND).await;
                    println!("end 1");
                })
            })
            .await;
        worker.close().await;
    }
}
