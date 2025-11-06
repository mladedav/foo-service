// @generated automatically by Diesel CLI.

diesel::table! {
    tasks (id) {
        id -> Int4,
        task_type -> Int4,
        state -> Int4,
        scheduled_at -> Timestamptz,
        shard -> Int2,
    }
}
