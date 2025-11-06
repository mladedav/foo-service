use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::{
    backend::Backend,
    deserialize,
    expression::AsExpression,
    prelude::*,
    serialize::{self, Output, ToSql},
    sql_types::Integer,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Insertable)]
#[diesel(table_name = crate::schema::tasks)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewTask {
    pub task_type: TaskType,
    pub scheduled_at: DateTime<Utc>,
}

impl NewTask {
    pub fn new(task_type: TaskType, scheduled_at: DateTime<Utc>) -> Self {
        Self {
            task_type,
            scheduled_at,
        }
    }
}

pub type TaskId = i32;

#[derive(Debug, Queryable, Selectable, Insertable, Serialize)]
#[diesel(table_name = crate::schema::tasks)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Task {
    pub id: TaskId,
    pub task_type: TaskType,
    pub state: TaskState,
    pub scheduled_at: NaiveDateTime,
    pub shard: i16,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, AsExpression, Serialize)]
#[diesel(sql_type = Integer)]
pub enum TaskType {
    Foo = 1,
    Bar = 2,
    Baz = 3,
}

impl ToSql<Integer, diesel::pg::Pg> for TaskType
where
    i32: ToSql<Integer, diesel::pg::Pg>,
{
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, diesel::pg::Pg>) -> serialize::Result {
        let v = *self as i32;
        <i32 as ToSql<Integer, diesel::pg::Pg>>::to_sql(&v, &mut out.reborrow())
    }
}

impl From<TaskType> for i32 {
    fn from(value: TaskType) -> i32 {
        value as i32
    }
}

impl<DB> Queryable<Integer, DB> for TaskType
where
    DB: Backend,
    i32: deserialize::FromSql<Integer, DB>,
{
    type Row = i32;

    fn build(value: i32) -> deserialize::Result<Self> {
        match value {
            1 => Ok(Self::Foo),
            2 => Ok(Self::Bar),
            3 => Ok(Self::Baz),
            _ => {
                tracing::error!(value = value, "Unexpected value of task type in database.");
                Err(Box::new(EnumDeserializationError {
                    value,
                    name: "TaskType",
                }))
            }
        }
    }
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, AsExpression, Serialize, Deserialize)]
#[diesel(sql_type = Integer)]
pub enum TaskState {
    Scheduled = 1,
    Started = 2,
    Done = 3,
}

impl ToSql<Integer, diesel::pg::Pg> for TaskState
where
    i32: ToSql<Integer, diesel::pg::Pg>,
{
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, diesel::pg::Pg>) -> serialize::Result {
        let v = *self as i32;
        <i32 as ToSql<Integer, diesel::pg::Pg>>::to_sql(&v, &mut out.reborrow())
    }
}

impl From<TaskState> for i32 {
    fn from(value: TaskState) -> i32 {
        value as i32
    }
}

impl<DB> Queryable<Integer, DB> for TaskState
where
    DB: Backend,
    i32: deserialize::FromSql<Integer, DB>,
{
    type Row = i32;

    fn build(value: i32) -> deserialize::Result<Self> {
        match value {
            1 => Ok(Self::Scheduled),
            2 => Ok(Self::Started),
            3 => Ok(Self::Done),
            _ => {
                tracing::error!(value = value, "Unexpected value of task type in database.");
                Err(Box::new(EnumDeserializationError {
                    value,
                    name: "TaskState",
                }))
            }
        }
    }
}

#[derive(Debug, Error)]
#[error("Unexpected task type value `{value}` when deserializing `{name}`")]
struct EnumDeserializationError {
    name: &'static str,
    value: i32,
}
