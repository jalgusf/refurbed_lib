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

/// A failed notification. Carries the original message so the caller can
/// resend it if it wants to; the library itself never retries.
#[derive(Debug, thiserror::Error)]
#[error("{kind}")]
pub struct DeliveryError {
    pub message: Bytes,
    pub kind: FailureKind,
}

#[derive(Debug, thiserror::Error)]
pub enum FailureKind {
    /// The server answered with a non-2xx status.
    #[error("server responded with {status}: {body}")]
    Status { status: StatusCode, body: String },
    /// No response within the configured timeout.
    #[error("request timed out")]
    Timeout,
    /// The server could not be reached.
    #[error("could not connect: {}", error_chain(.0))]
    Connect(reqwest::Error),
    /// Any other transport error, e.g. the connection dropped mid-request.
    #[error("request failed: {}", error_chain(.0))]
    Request(reqwest::Error),
}

impl DeliveryError {
    /// Whether resending might succeed. False only for 4xx responses other
    /// than 429, where the server rejected the message itself.
    pub fn is_retryable(&self) -> bool {
        match &self.kind {
            FailureKind::Status { status, .. } => {
                !status.is_client_error() || *status == StatusCode::TOO_MANY_REQUESTS
            }
            FailureKind::Timeout | FailureKind::Connect(_) | FailureKind::Request(_) => true,
        }
    }
}

/// reqwest's own message is often just "error sending request"; the actual
/// cause (connection refused, DNS failure, ...) is further down the chain.
fn error_chain(err: &dyn std::error::Error) -> String {
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
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
            let request = client.post(url.clone()).body(job.body.clone());
            async move {
                let result = send(request).await.map_err(|kind| DeliveryError { message: job.body, kind });
                // The caller may have dropped its Delivery; that's fine.
                let _ = job.done.send(result);
            }
        })
        .await;
}

async fn send(request: reqwest::RequestBuilder) -> Result<(), FailureKind> {
    let classify = |err: reqwest::Error| {
        if err.is_timeout() {
            FailureKind::Timeout
        } else if err.is_connect() {
            FailureKind::Connect(err)
        } else {
            FailureKind::Request(err)
        }
    };
    let resp = request.send().await.map_err(classify)?;
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let body = resp.text().await.map_err(classify)?;
    Err(FailureKind::Status { status, body })
}
