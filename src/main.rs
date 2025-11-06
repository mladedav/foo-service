use std::{io, num::NonZeroU8};

use axum::routing::{get, post};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use dotenvy::dotenv;
use handlers::{create_task, get_task, list_tasks, remove_task};
use models::{Task, TaskState};
use tokio::{net::TcpListener, sync::mpsc};
use tracing::Level;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod handlers;
mod models;
mod processor;
mod schema;
mod sharding;

pub async fn establish_connection() -> AsyncPgConnection {
    dotenv().ok();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    AsyncPgConnection::establish(&database_url)
        .await
        .unwrap_or_else(|_| panic!("Error connecting to {}", database_url))
}

#[derive(Clone)]
struct AppState {
    this_instance: u8,
    instance_count: NonZeroU8,
    task_processor: mpsc::Sender<Task>,
}

impl AppState {
    async fn add_task(&self, task: Task) {
        match self.task_processor.send(task).await {
            Ok(()) => {}
            Err(error) => {
                tracing::error!(error = display(error), "Unable to enqueue a new task.");
            }
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // TODO get these from environment. And make them updatable in runtime.
    let this_instance = 0;
    let instance_count = NonZeroU8::new(1).unwrap();

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::filter::Targets::new()
                .with_default(Level::INFO)
                .with_target("foo-service", Level::DEBUG),
        )
        .init();

    let (task_tx, task_rx) = mpsc::channel(1024);
    let state = AppState {
        this_instance,
        instance_count,
        task_processor: task_tx,
    };

    let listener = TcpListener::bind("127.0.0.1:3000")
        .await
        .map_err(|error| io::Error::other(format!("could not bind port: {error}")))?;

    // TODO reuse connections in a pool
    let mut connection = establish_connection().await;
    let mut initial_tasks = schema::tasks::table
        .filter(schema::tasks::state.eq(TaskState::Scheduled))
        .load::<Task>(&mut connection)
        .await
        .map_err(io::Error::other)?;
    tracing::debug!(count = initial_tasks.len(), "Loaded all scheduled tasks.");
    // This is suboptimal, we should first enumerate our shards and load just those.
    initial_tasks.retain(|task| sharding::is_local(this_instance, instance_count, task.shard));
    tracing::debug!(count = initial_tasks.len(), "Filtered local tasks.");

    let _processor = tokio::spawn(processor::process(initial_tasks, task_rx));

    let router = axum::Router::new()
        .route("/task", post(create_task).get(list_tasks))
        .route("/task/{id}", get(get_task).delete(remove_task))
        .with_state(state);

    axum::serve(listener, router).await
}
