use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use chrono::{DateTime, Utc};
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use http::HeaderValue;
use serde::Deserialize;

use crate::{
    AppState, establish_connection,
    models::{NewTask, TaskId, TaskState},
    sharding,
};
use crate::{
    models::{Task, TaskType as DbTaskType},
    schema,
    schema::tasks::dsl::*,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")] // just so we have `foo` instead of `Foo`
enum TaskType {
    Foo,
    Bar,
    Baz,
}

impl From<TaskType> for DbTaskType {
    fn from(value: TaskType) -> Self {
        match value {
            TaskType::Foo => DbTaskType::Foo,
            TaskType::Bar => DbTaskType::Bar,
            TaskType::Baz => DbTaskType::Baz,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateQuery {
    #[serde(rename = "type")]
    task_type: TaskType,
    start_at: DateTime<Utc>,
}

pub async fn create_task(
    State(app_state): State<AppState>,
    Query(query): Query<CreateQuery>,
) -> impl IntoResponse {
    let CreateQuery {
        task_type: task_type_,
        start_at,
    } = query;

    let new_task = NewTask::new(DbTaskType::from(task_type_), start_at);

    // TODO use connection pool and pass it around through state.
    let mut conn = establish_connection().await;

    let Ok(task) = diesel::insert_into(schema::tasks::table)
        .values(&new_task)
        .returning(Task::as_returning())
        .get_result(&mut conn)
        .await
        .inspect_err(|error| {
            tracing::error!(
                error = display(error),
                "Failed inserting a new task into database."
            );
        })
    else {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };

    let location_value = HeaderValue::from_str(&format!("/task/{}", task.id)).map_err(|error| {
        tracing::error!(
            task.id = display(task.id),
            error = display(error),
            "Unable to construct the `location` header value."
        );
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if sharding::is_local(
        app_state.this_instance,
        app_state.instance_count,
        task.shard,
    ) {
        app_state.add_task(task).await;
    } else {
        // TODO Gossip/notify the correct instance or just use SQL LISTEN/NOTIFY so that whoever is
        // interested finds out.
    }

    let mut headers = HeaderMap::new();
    headers.append(http::header::LOCATION, location_value);

    Ok((StatusCode::CREATED, headers))
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    #[serde(rename = "type")]
    task_type: Option<TaskType>,
    state: Option<TaskState>,
}

pub async fn list_tasks(Query(params): Query<ListQuery>) -> impl IntoResponse {
    // TODO use connection pool and pass it around through state.
    let mut conn = establish_connection().await;

    let mut query = schema::tasks::table.into_boxed();

    if let Some(task_filter) = params.task_type {
        let task_filter = DbTaskType::from(task_filter);
        query = query.filter(schema::tasks::task_type.eq(task_filter));
    }

    if let Some(state_filter) = params.state {
        query = query.filter(schema::tasks::state.eq(state_filter));
    }

    query
        .load::<Task>(&mut conn)
        .await
        .map(Json)
        .map_err(|error| {
            tracing::error!(
                error = display(error),
                "Unable to communicate with db to list entities."
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

pub async fn get_task(Path(task_id): Path<TaskId>) -> Result<Json<Task>, StatusCode> {
    // TODO use connection pool and pass it around through state.
    let mut conn = establish_connection().await;

    let task = tasks
        .filter(id.eq(task_id))
        .load::<Task>(&mut conn)
        .await
        .map_err(|error| {
            tracing::error!(
                error = display(error),
                "Unable to communicate with db to get entity."
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .pop()
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(task))
}

pub async fn remove_task(Path(task_id): Path<TaskId>) -> Result<StatusCode, StatusCode> {
    // TODO use connection pool and pass it around through state.
    let mut conn = establish_connection().await;

    let result = diesel::delete(tasks.filter(id.eq(task_id)))
        .execute(&mut conn)
        .await
        .map_err(|error| {
            tracing::error!(
                error = display(error),
                "Unable to communicate with db to delete entity."
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if result == 0 {
        Ok(StatusCode::NOT_FOUND)
    } else {
        Ok(StatusCode::NO_CONTENT)
    }
}
