use std::{collections::BinaryHeap, pin::pin, time::Duration};

use chrono::Utc;
use diesel::{BoolExpressionMethods, ExpressionMethods, QueryDsl};
use futures_util::future::{Either, select};
use tokio::sync::mpsc;

use diesel_async::RunQueryDsl;

use crate::{
    establish_connection,
    models::{Task, TaskState, TaskType},
    schema::{
        self,
        tasks::{dsl::*, state},
    },
};

pub async fn process(initial_tasks: Vec<Task>, mut new_tasks: mpsc::Receiver<Task>) {
    let mut pending_tasks = BinaryHeap::<TimeOrderedTask>::with_capacity(initial_tasks.len());
    pending_tasks.extend(initial_tasks.into_iter().map(TimeOrderedTask));

    loop {
        let recv_future = pin!(new_tasks.recv());

        let new_task = match pending_tasks.peek() {
            None => recv_future.await,
            Some(next_task) => {
                let delay = next_task
                    .0
                    .scheduled_at
                    .signed_duration_since(Utc::now().naive_utc());
                let delay = delay.to_std().unwrap_or(Duration::ZERO);
                let sleep = pin!(tokio::time::sleep(delay));

                match select(sleep, recv_future).await {
                    Either::Left(((), _)) => {
                        let ready_task =
                            pending_tasks.pop().expect("we've peeked this, it's there");
                        process_task(ready_task.0).await;
                        continue;
                    }
                    Either::Right((new_task, _)) => new_task,
                }
            }
        };

        let Some(new_task) = new_task else {
            tracing::info!("New tasks channel was closed, shutting down the processing task.");
            return;
        };

        let start_at = new_task.scheduled_at;
        if start_at < Utc::now().naive_utc() {
            tracing::warn!(
                scheduled.at = display(start_at),
                "Processing a task after it has been scheduled."
            );
            process_task(new_task).await;
        } else {
            pending_tasks.push(TimeOrderedTask(new_task));
        }
    }
}

async fn process_task(task: Task) {
    tracing::debug!(task = debug(&task), "Processing task.");
    // Ideally, we'd use a pool here too.
    let mut connection = establish_connection().await;

    // The assignment said to process each task only once so we're going with at most once rather
    // than at least once semantics. We first mark the state as being started and if the application
    // crashes right now, the task will most likely never run. But since we'll see that it is
    // `Started` and not `Finished`, we at least know which tasks _maybe_ did not run.
    match diesel::update(
        tasks.filter(
            schema::tasks::id
                .eq(task.id)
                .and(schema::tasks::state.eq(TaskState::Scheduled)),
        ),
    )
    .set(state.eq(TaskState::Started))
    .execute(&mut connection)
    .await
    {
        Ok(0) => {
            // The task was either deleted or it has already been started by someone. We just ignore this.
            return;
        }
        Ok(1) => {
            // This is what we like to see.
        }
        Ok(_) => {
            tracing::error!(
                "Unexpected number of tasks affected in a query that should touch one or none."
            );
            // This should be unreachable and there's no telling what state we're in. But if
            // we're not going to shut down the whole application, we can finish the task we
            // thought we were going to do.
        }
        Err(error) => {
            tracing::error!(
                error = display(error),
                task.id = display(task.id),
                "Failed updating state of a task in database."
            );
            // We might want to put the task somewhere to be retried later again. This way we forgot about it but we will load it once more when we load the whole state.
            return;
        }
    }

    tokio::spawn(async move {
        match task.task_type {
            TaskType::Foo => {
                tokio::time::sleep(Duration::from_secs(3)).await;
                println!("Foo {}", task.id);
            }
            TaskType::Bar => {
                // TODO reuse the client between tasks. Connection usage aside, loading
                // certificates can be surprisingly costly.
                let client = reqwest::Client::new();
                let mut success = false;
                for retry in 0..10 {
                    match client
                        .get("https://www.whattimeisitrightnow.com/")
                        .send()
                        .await
                    {
                        Ok(response) => {
                            println!("{}", response.status());
                            success = true;
                            break;
                        }
                        Err(error) => {
                            tracing::warn!(
                                task.id = display(task.id),
                                error = display(error),
                                "Failed sending request for bar task."
                            );
                        }
                    }

                    // TODO better retries, better limits, whatever we want to support.
                    tokio::time::sleep(Duration::from_millis(50 + 2u64.pow(retry))).await;
                }
                if !success {
                    tracing::error!(
                        task.type = debug(task.task_type),
                        task.id = display(task.id),
                        "Failed to process a task."
                    );

                    // TODO we could add a failed state.
                    return;
                }
            }
            TaskType::Baz => {
                // I assume we don't care, but this is going to be very slightly skewed in favor of
                // [0..22] because of how modulo works.
                let random = fastrand::u16(0..=343);
                println!("Baz {random}");
            }
        }

        _ = diesel::update(tasks.filter(schema::tasks::id.eq(task.id)))
            .set(state.eq(TaskState::Done))
            .execute(&mut connection)
            .await
            .inspect_err(|error| {
                tracing::error!(
                    error = display(error),
                    task.id = display(task.id),
                    "Failed updating state of a task in database after finishing."
                )
            });
    });
}

/// This is just a wrapper that provides an ordering based on the schedule that is reversed so that
/// we can use this with the std binary heap.
struct TimeOrderedTask(Task);

impl PartialEq for TimeOrderedTask {
    fn eq(&self, other: &Self) -> bool {
        self.0.scheduled_at == other.0.scheduled_at
    }
}

impl Eq for TimeOrderedTask {}

impl PartialOrd for TimeOrderedTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimeOrderedTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.scheduled_at.cmp(&other.0.scheduled_at).reverse()
    }
}
