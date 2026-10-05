//! Non-blocking HTTP notification client.
//!
//! Messages are pushed into a bounded queue and POSTed to a fixed URL by a
//! background task with at most `max_in_flight` concurrent requests.

use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use reqwest::{Client, StatusCode};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub use reqwest::Url;

pub struct Config {
    /// Where notifications are POSTed.
    pub url: Url,
    /// Upper bound on concurrent requests (and therefore open sockets).
    pub max_in_flight: usize,
    /// How many messages may wait in the queue before `notify` rejects.
    pub queue_capacity: usize,
    /// Per-request timeout, so a hung request can't hold a slot forever.
    pub timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum NotifyError {
    /// The queue is full; the message is handed back so the caller can retry.
    #[error("notification queue is full")]
    QueueFull(Bytes),
    /// The notifier has been shut down.
    #[error("notifier is closed")]
    Closed(Bytes),
}

#[derive(Debug, thiserror::Error)]
pub enum DeliveryError {
    #[error("server responded with {0}")]
    Status(StatusCode),
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
}

/// Resolves to the outcome of a single notification. Drop it to fire-and-forget.
pub type Delivery = oneshot::Receiver<Result<(), DeliveryError>>;

struct Job {
    body: Bytes,
    done: oneshot::Sender<Result<(), DeliveryError>>,
}

pub struct Notifier {
    tx: mpsc::Sender<Job>,
    worker: JoinHandle<()>,
}

impl Notifier {
    /// Spawns the dispatcher task; must be called from within a Tokio runtime.
    pub fn new(config: Config) -> Result<Self, reqwest::Error> {
        let client = Client::builder()
            .timeout(config.timeout)
            .pool_max_idle_per_host(config.max_in_flight)
            .build()?;
        let (tx, rx) = mpsc::channel(config.queue_capacity);
        let worker = tokio::spawn(dispatch(rx, client, config.url, config.max_in_flight));
        Ok(Self { tx, worker })
    }

    /// Queues a message for delivery without blocking.
    pub fn notify(&self, message: impl Into<Bytes>) -> Result<Delivery, NotifyError> {
        let (done, delivery) = oneshot::channel();
        let job = Job { body: message.into(), done };
        self.tx.try_send(job).map_err(|err| match err {
            mpsc::error::TrySendError::Full(job) => NotifyError::QueueFull(job.body),
            mpsc::error::TrySendError::Closed(job) => NotifyError::Closed(job.body),
        })?;
        Ok(delivery)
    }

    /// Stops accepting messages and waits until all queued ones are sent.
    pub async fn shutdown(self) {
        drop(self.tx);
        let _ = self.worker.await;
    }
}

async fn dispatch(mut rx: mpsc::Receiver<Job>, client: Client, url: Url, max_in_flight: usize) {
    futures::stream::poll_fn(|cx| rx.poll_recv(cx))
        .for_each_concurrent(max_in_flight, |job| {
            let request = client.post(url.clone()).body(job.body);
            async move {
                let result = match request.send().await {
                    Ok(resp) if resp.status().is_success() => Ok(()),
                    Ok(resp) => Err(DeliveryError::Status(resp.status())),
                    Err(err) => Err(err.into()),
                };
                // The caller may have dropped its Delivery; that's fine.
                let _ = job.done.send(result);
            }
        })
        .await;
}
