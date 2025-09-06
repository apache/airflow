// @generated automatically by Diesel CLI.

diesel::table! {
    alembic_version (version_num) {
        #[max_length = 32]
        version_num -> Varchar,
    }
}

diesel::table! {
    asset (id) {
        id -> Int4,
        #[max_length = 1500]
        name -> Varchar,
        #[max_length = 1500]
        uri -> Varchar,
        #[max_length = 1500]
        group -> Varchar,
        extra -> Json,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    asset_active (name, uri) {
        #[max_length = 1500]
        name -> Varchar,
        #[max_length = 1500]
        uri -> Varchar,
    }
}

diesel::table! {
    asset_alias (id) {
        id -> Int4,
        #[max_length = 1500]
        name -> Varchar,
        #[max_length = 1500]
        group -> Varchar,
    }
}

diesel::table! {
    asset_alias_asset (alias_id, asset_id) {
        alias_id -> Int4,
        asset_id -> Int4,
    }
}

diesel::table! {
    asset_alias_asset_event (alias_id, event_id) {
        alias_id -> Int4,
        event_id -> Int4,
    }
}

diesel::table! {
    asset_dag_run_queue (asset_id, target_dag_id) {
        asset_id -> Int4,
        #[max_length = 250]
        target_dag_id -> Varchar,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    asset_event (id) {
        id -> Int4,
        asset_id -> Int4,
        extra -> Json,
        #[max_length = 250]
        source_task_id -> Nullable<Varchar>,
        #[max_length = 250]
        source_dag_id -> Nullable<Varchar>,
        #[max_length = 250]
        source_run_id -> Nullable<Varchar>,
        source_map_index -> Nullable<Int4>,
        timestamp -> Timestamptz,
    }
}

diesel::table! {
    asset_trigger (asset_id, trigger_id) {
        asset_id -> Int4,
        trigger_id -> Int4,
    }
}

diesel::table! {
    backfill (id) {
        id -> Int4,
        #[max_length = 250]
        dag_id -> Varchar,
        from_date -> Timestamptz,
        to_date -> Timestamptz,
        dag_run_conf -> Json,
        is_paused -> Nullable<Bool>,
        #[max_length = 250]
        reprocess_behavior -> Varchar,
        max_active_runs -> Int4,
        created_at -> Timestamptz,
        completed_at -> Nullable<Timestamptz>,
        updated_at -> Timestamptz,
        #[max_length = 512]
        triggering_user_name -> Nullable<Varchar>,
    }
}

diesel::table! {
    backfill_dag_run (id) {
        id -> Int4,
        backfill_id -> Int4,
        dag_run_id -> Nullable<Int4>,
        #[max_length = 250]
        exception_reason -> Nullable<Varchar>,
        logical_date -> Timestamptz,
        sort_ordinal -> Int4,
    }
}

diesel::table! {
    callback_request (id) {
        id -> Int4,
        created_at -> Timestamptz,
        priority_weight -> Int4,
        callback_data -> Jsonb,
        #[max_length = 20]
        callback_type -> Varchar,
    }
}

diesel::table! {
    connection (id) {
        id -> Int4,
        #[max_length = 250]
        conn_id -> Varchar,
        #[max_length = 500]
        conn_type -> Varchar,
        description -> Nullable<Text>,
        #[max_length = 500]
        host -> Nullable<Varchar>,
        #[max_length = 500]
        schema -> Nullable<Varchar>,
        login -> Nullable<Text>,
        password -> Nullable<Text>,
        port -> Nullable<Int4>,
        is_encrypted -> Nullable<Bool>,
        is_extra_encrypted -> Nullable<Bool>,
        extra -> Nullable<Text>,
    }
}

diesel::table! {
    dag (dag_id) {
        #[max_length = 250]
        dag_id -> Varchar,
        is_paused -> Nullable<Bool>,
        is_stale -> Nullable<Bool>,
        last_parsed_time -> Nullable<Timestamptz>,
        last_expired -> Nullable<Timestamptz>,
        #[max_length = 2000]
        fileloc -> Nullable<Varchar>,
        #[max_length = 2000]
        relative_fileloc -> Nullable<Varchar>,
        #[max_length = 250]
        bundle_name -> Nullable<Varchar>,
        #[max_length = 200]
        bundle_version -> Nullable<Varchar>,
        #[max_length = 2000]
        owners -> Nullable<Varchar>,
        #[max_length = 2000]
        dag_display_name -> Nullable<Varchar>,
        description -> Nullable<Text>,
        timetable_summary -> Nullable<Text>,
        #[max_length = 1000]
        timetable_description -> Nullable<Varchar>,
        asset_expression -> Nullable<Json>,
        deadline -> Nullable<Json>,
        max_active_tasks -> Int4,
        max_active_runs -> Nullable<Int4>,
        max_consecutive_failed_dag_runs -> Int4,
        has_task_concurrency_limits -> Bool,
        has_import_errors -> Nullable<Bool>,
        next_dagrun -> Nullable<Timestamptz>,
        next_dagrun_data_interval_start -> Nullable<Timestamptz>,
        next_dagrun_data_interval_end -> Nullable<Timestamptz>,
        next_dagrun_create_after -> Nullable<Timestamptz>,
    }
}

diesel::table! {
    dag_bundle (name) {
        #[max_length = 250]
        name -> Varchar,
        active -> Nullable<Bool>,
        #[max_length = 200]
        version -> Nullable<Varchar>,
        last_refreshed -> Nullable<Timestamptz>,
    }
}

diesel::table! {
    dag_code (id) {
        id -> Uuid,
        #[max_length = 250]
        dag_id -> Varchar,
        #[max_length = 2000]
        fileloc -> Varchar,
        created_at -> Timestamptz,
        last_updated -> Timestamptz,
        source_code -> Text,
        #[max_length = 32]
        source_code_hash -> Varchar,
        dag_version_id -> Uuid,
    }
}

diesel::table! {
    dag_favorite (user_id, dag_id) {
        #[max_length = 250]
        user_id -> Varchar,
        #[max_length = 250]
        dag_id -> Varchar,
    }
}

diesel::table! {
    dag_owner_attributes (dag_id, owner) {
        #[max_length = 250]
        dag_id -> Varchar,
        #[max_length = 500]
        owner -> Varchar,
        #[max_length = 500]
        link -> Varchar,
    }
}

diesel::table! {
    dag_priority_parsing_request (id) {
        #[max_length = 32]
        id -> Varchar,
        #[max_length = 250]
        bundle_name -> Varchar,
        #[max_length = 2000]
        relative_fileloc -> Varchar,
    }
}

diesel::table! {
    dag_run (id) {
        id -> Int4,
        #[max_length = 250]
        dag_id -> Varchar,
        queued_at -> Nullable<Timestamptz>,
        logical_date -> Nullable<Timestamptz>,
        start_date -> Nullable<Timestamptz>,
        end_date -> Nullable<Timestamptz>,
        #[max_length = 50]
        state -> Nullable<Varchar>,
        #[max_length = 250]
        run_id -> Varchar,
        creating_job_id -> Nullable<Int4>,
        #[max_length = 50]
        run_type -> Varchar,
        #[max_length = 50]
        triggered_by -> Nullable<Varchar>,
        #[max_length = 512]
        triggering_user_name -> Nullable<Varchar>,
        conf -> Nullable<Jsonb>,
        data_interval_start -> Nullable<Timestamptz>,
        data_interval_end -> Nullable<Timestamptz>,
        run_after -> Timestamptz,
        last_scheduling_decision -> Nullable<Timestamptz>,
        log_template_id -> Nullable<Int4>,
        updated_at -> Nullable<Timestamptz>,
        clear_number -> Int4,
        backfill_id -> Nullable<Int4>,
        #[max_length = 250]
        bundle_version -> Nullable<Varchar>,
        scheduled_by_job_id -> Nullable<Int4>,
        context_carrier -> Nullable<Jsonb>,
        #[max_length = 250]
        span_status -> Varchar,
        created_dag_version_id -> Nullable<Uuid>,
    }
}

diesel::table! {
    dag_run_note (dag_run_id) {
        #[max_length = 128]
        user_id -> Nullable<Varchar>,
        dag_run_id -> Int4,
        #[max_length = 1000]
        content -> Nullable<Varchar>,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    dag_schedule_asset_alias_reference (alias_id, dag_id) {
        alias_id -> Int4,
        #[max_length = 250]
        dag_id -> Varchar,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    dag_schedule_asset_name_reference (name, dag_id) {
        #[max_length = 1500]
        name -> Varchar,
        #[max_length = 250]
        dag_id -> Varchar,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    dag_schedule_asset_reference (asset_id, dag_id) {
        asset_id -> Int4,
        #[max_length = 250]
        dag_id -> Varchar,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    dag_schedule_asset_uri_reference (uri, dag_id) {
        #[max_length = 1500]
        uri -> Varchar,
        #[max_length = 250]
        dag_id -> Varchar,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    dag_tag (name, dag_id) {
        #[max_length = 100]
        name -> Varchar,
        #[max_length = 250]
        dag_id -> Varchar,
    }
}

diesel::table! {
    dag_version (id) {
        id -> Uuid,
        version_number -> Int4,
        #[max_length = 250]
        dag_id -> Varchar,
        #[max_length = 250]
        bundle_name -> Nullable<Varchar>,
        #[max_length = 250]
        bundle_version -> Nullable<Varchar>,
        created_at -> Timestamptz,
        last_updated -> Timestamptz,
    }
}

diesel::table! {
    dag_warning (dag_id, warning_type) {
        #[max_length = 250]
        dag_id -> Varchar,
        #[max_length = 50]
        warning_type -> Varchar,
        message -> Text,
        timestamp -> Timestamptz,
    }
}

diesel::table! {
    dagrun_asset_event (dag_run_id, event_id) {
        dag_run_id -> Int4,
        event_id -> Int4,
    }
}

diesel::table! {
    deadline (id) {
        id -> Uuid,
        #[max_length = 250]
        dag_id -> Nullable<Varchar>,
        dagrun_id -> Nullable<Int4>,
        deadline_time -> Timestamptz,
        #[max_length = 500]
        callback -> Varchar,
        callback_kwargs -> Nullable<Json>,
    }
}

diesel::table! {
    import_error (id) {
        id -> Int4,
        timestamp -> Nullable<Timestamptz>,
        #[max_length = 1024]
        filename -> Nullable<Varchar>,
        #[max_length = 250]
        bundle_name -> Nullable<Varchar>,
        stacktrace -> Nullable<Text>,
    }
}

diesel::table! {
    job (id) {
        id -> Int4,
        #[max_length = 250]
        dag_id -> Nullable<Varchar>,
        #[max_length = 20]
        state -> Nullable<Varchar>,
        #[max_length = 30]
        job_type -> Nullable<Varchar>,
        start_date -> Nullable<Timestamptz>,
        end_date -> Nullable<Timestamptz>,
        latest_heartbeat -> Nullable<Timestamptz>,
        #[max_length = 500]
        executor_class -> Nullable<Varchar>,
        #[max_length = 500]
        hostname -> Nullable<Varchar>,
        #[max_length = 1000]
        unixname -> Nullable<Varchar>,
    }
}

diesel::table! {
    log (id) {
        id -> Int4,
        dttm -> Nullable<Timestamptz>,
        #[max_length = 250]
        dag_id -> Nullable<Varchar>,
        #[max_length = 250]
        task_id -> Nullable<Varchar>,
        map_index -> Nullable<Int4>,
        #[max_length = 60]
        event -> Nullable<Varchar>,
        logical_date -> Nullable<Timestamptz>,
        #[max_length = 250]
        run_id -> Nullable<Varchar>,
        #[max_length = 500]
        owner -> Nullable<Varchar>,
        #[max_length = 500]
        owner_display_name -> Nullable<Varchar>,
        extra -> Nullable<Text>,
        try_number -> Nullable<Int4>,
    }
}

diesel::table! {
    log_template (id) {
        id -> Int4,
        filename -> Text,
        elasticsearch_id -> Text,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    rendered_task_instance_fields (dag_id, task_id, run_id, map_index) {
        #[max_length = 250]
        dag_id -> Varchar,
        #[max_length = 250]
        task_id -> Varchar,
        #[max_length = 250]
        run_id -> Varchar,
        map_index -> Int4,
        rendered_fields -> Json,
        k8s_pod_yaml -> Nullable<Json>,
    }
}

diesel::table! {
    serialized_dag (id) {
        id -> Uuid,
        #[max_length = 250]
        dag_id -> Varchar,
        data -> Nullable<Json>,
        data_compressed -> Nullable<Bytea>,
        created_at -> Timestamptz,
        last_updated -> Timestamptz,
        #[max_length = 32]
        dag_hash -> Varchar,
        dag_version_id -> Uuid,
    }
}

diesel::table! {
    slot_pool (id) {
        id -> Int4,
        #[max_length = 256]
        pool -> Nullable<Varchar>,
        slots -> Nullable<Int4>,
        description -> Nullable<Text>,
        include_deferred -> Bool,
    }
}

diesel::table! {
    task_inlet_asset_reference (asset_id, dag_id, task_id) {
        asset_id -> Int4,
        #[max_length = 250]
        dag_id -> Varchar,
        #[max_length = 250]
        task_id -> Varchar,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    task_instance (id) {
        id -> Uuid,
        #[max_length = 250]
        task_id -> Varchar,
        #[max_length = 250]
        dag_id -> Varchar,
        #[max_length = 250]
        run_id -> Varchar,
        map_index -> Int4,
        start_date -> Nullable<Timestamptz>,
        end_date -> Nullable<Timestamptz>,
        duration -> Nullable<Float8>,
        #[max_length = 20]
        state -> Nullable<Varchar>,
        try_number -> Nullable<Int4>,
        max_tries -> Nullable<Int4>,
        #[max_length = 1000]
        hostname -> Nullable<Varchar>,
        #[max_length = 1000]
        unixname -> Nullable<Varchar>,
        #[max_length = 256]
        pool -> Varchar,
        pool_slots -> Int4,
        #[max_length = 256]
        queue -> Nullable<Varchar>,
        priority_weight -> Nullable<Int4>,
        #[max_length = 1000]
        operator -> Nullable<Varchar>,
        #[max_length = 1000]
        custom_operator_name -> Nullable<Varchar>,
        queued_dttm -> Nullable<Timestamptz>,
        scheduled_dttm -> Nullable<Timestamptz>,
        queued_by_job_id -> Nullable<Int4>,
        last_heartbeat_at -> Nullable<Timestamptz>,
        pid -> Nullable<Int4>,
        #[max_length = 1000]
        executor -> Nullable<Varchar>,
        executor_config -> Nullable<Bytea>,
        updated_at -> Nullable<Timestamptz>,
        #[max_length = 250]
        rendered_map_index -> Nullable<Varchar>,
        context_carrier -> Nullable<Jsonb>,
        #[max_length = 250]
        span_status -> Varchar,
        #[max_length = 250]
        external_executor_id -> Nullable<Varchar>,
        trigger_id -> Nullable<Int4>,
        trigger_timeout -> Nullable<Timestamptz>,
        #[max_length = 1000]
        next_method -> Nullable<Varchar>,
        next_kwargs -> Nullable<Jsonb>,
        #[max_length = 2000]
        task_display_name -> Nullable<Varchar>,
        dag_version_id -> Nullable<Uuid>,
    }
}

diesel::table! {
    task_instance_history (task_instance_id) {
        task_instance_id -> Uuid,
        #[max_length = 250]
        task_id -> Varchar,
        #[max_length = 250]
        dag_id -> Varchar,
        #[max_length = 250]
        run_id -> Varchar,
        map_index -> Int4,
        try_number -> Int4,
        start_date -> Nullable<Timestamptz>,
        end_date -> Nullable<Timestamptz>,
        duration -> Nullable<Float8>,
        #[max_length = 20]
        state -> Nullable<Varchar>,
        max_tries -> Nullable<Int4>,
        #[max_length = 1000]
        hostname -> Nullable<Varchar>,
        #[max_length = 1000]
        unixname -> Nullable<Varchar>,
        #[max_length = 256]
        pool -> Varchar,
        pool_slots -> Int4,
        #[max_length = 256]
        queue -> Nullable<Varchar>,
        priority_weight -> Nullable<Int4>,
        #[max_length = 1000]
        operator -> Nullable<Varchar>,
        #[max_length = 1000]
        custom_operator_name -> Nullable<Varchar>,
        queued_dttm -> Nullable<Timestamptz>,
        scheduled_dttm -> Nullable<Timestamptz>,
        queued_by_job_id -> Nullable<Int4>,
        pid -> Nullable<Int4>,
        #[max_length = 1000]
        executor -> Nullable<Varchar>,
        executor_config -> Nullable<Bytea>,
        updated_at -> Nullable<Timestamptz>,
        #[max_length = 250]
        rendered_map_index -> Nullable<Varchar>,
        context_carrier -> Nullable<Jsonb>,
        #[max_length = 250]
        span_status -> Varchar,
        #[max_length = 250]
        external_executor_id -> Nullable<Varchar>,
        trigger_id -> Nullable<Int4>,
        trigger_timeout -> Nullable<Timestamp>,
        #[max_length = 1000]
        next_method -> Nullable<Varchar>,
        next_kwargs -> Nullable<Jsonb>,
        #[max_length = 2000]
        task_display_name -> Nullable<Varchar>,
        dag_version_id -> Nullable<Uuid>,
    }
}

diesel::table! {
    task_instance_note (ti_id) {
        ti_id -> Uuid,
        #[max_length = 128]
        user_id -> Nullable<Varchar>,
        #[max_length = 1000]
        content -> Nullable<Varchar>,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    task_map (dag_id, task_id, run_id, map_index) {
        #[max_length = 250]
        dag_id -> Varchar,
        #[max_length = 250]
        task_id -> Varchar,
        #[max_length = 250]
        run_id -> Varchar,
        map_index -> Int4,
        length -> Int4,
        keys -> Nullable<Jsonb>,
    }
}

diesel::table! {
    task_outlet_asset_reference (asset_id, dag_id, task_id) {
        asset_id -> Int4,
        #[max_length = 250]
        dag_id -> Varchar,
        #[max_length = 250]
        task_id -> Varchar,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    task_reschedule (id) {
        id -> Int4,
        ti_id -> Uuid,
        start_date -> Timestamptz,
        end_date -> Timestamptz,
        duration -> Int4,
        reschedule_date -> Timestamptz,
    }
}

diesel::table! {
    trigger (id) {
        id -> Int4,
        #[max_length = 1000]
        classpath -> Varchar,
        kwargs -> Text,
        created_date -> Timestamptz,
        triggerer_id -> Nullable<Int4>,
    }
}

diesel::table! {
    variable (id) {
        id -> Int4,
        #[max_length = 250]
        key -> Nullable<Varchar>,
        val -> Nullable<Text>,
        description -> Nullable<Text>,
        is_encrypted -> Nullable<Bool>,
    }
}

diesel::table! {
    xcom (dag_run_id, task_id, map_index, key) {
        dag_run_id -> Int4,
        #[max_length = 250]
        task_id -> Varchar,
        map_index -> Int4,
        #[max_length = 512]
        key -> Varchar,
        #[max_length = 250]
        dag_id -> Varchar,
        #[max_length = 250]
        run_id -> Varchar,
        value -> Nullable<Jsonb>,
        timestamp -> Timestamptz,
    }
}

diesel::joinable!(asset_alias_asset -> asset (asset_id));
diesel::joinable!(asset_alias_asset -> asset_alias (alias_id));
diesel::joinable!(asset_alias_asset_event -> asset_alias (alias_id));
diesel::joinable!(asset_alias_asset_event -> asset_event (event_id));
diesel::joinable!(asset_dag_run_queue -> asset (asset_id));
diesel::joinable!(asset_dag_run_queue -> dag (target_dag_id));
diesel::joinable!(asset_trigger -> asset (asset_id));
diesel::joinable!(asset_trigger -> trigger (trigger_id));
diesel::joinable!(backfill_dag_run -> backfill (backfill_id));
diesel::joinable!(backfill_dag_run -> dag_run (dag_run_id));
diesel::joinable!(dag -> dag_bundle (bundle_name));
diesel::joinable!(dag_code -> dag_version (dag_version_id));
diesel::joinable!(dag_favorite -> dag (dag_id));
diesel::joinable!(dag_owner_attributes -> dag (dag_id));
diesel::joinable!(dag_run -> backfill (backfill_id));
diesel::joinable!(dag_run -> dag_version (created_dag_version_id));
diesel::joinable!(dag_run -> log_template (log_template_id));
diesel::joinable!(dag_run_note -> dag_run (dag_run_id));
diesel::joinable!(dag_schedule_asset_alias_reference -> asset_alias (alias_id));
diesel::joinable!(dag_schedule_asset_alias_reference -> dag (dag_id));
diesel::joinable!(dag_schedule_asset_name_reference -> dag (dag_id));
diesel::joinable!(dag_schedule_asset_reference -> asset (asset_id));
diesel::joinable!(dag_schedule_asset_reference -> dag (dag_id));
diesel::joinable!(dag_schedule_asset_uri_reference -> dag (dag_id));
diesel::joinable!(dag_tag -> dag (dag_id));
diesel::joinable!(dag_version -> dag (dag_id));
diesel::joinable!(dag_warning -> dag (dag_id));
diesel::joinable!(dagrun_asset_event -> asset_event (event_id));
diesel::joinable!(dagrun_asset_event -> dag_run (dag_run_id));
diesel::joinable!(deadline -> dag (dag_id));
diesel::joinable!(deadline -> dag_run (dagrun_id));
diesel::joinable!(serialized_dag -> dag_version (dag_version_id));
diesel::joinable!(task_inlet_asset_reference -> asset (asset_id));
diesel::joinable!(task_inlet_asset_reference -> dag (dag_id));
diesel::joinable!(task_instance -> dag_version (dag_version_id));
diesel::joinable!(task_instance -> trigger (trigger_id));
diesel::joinable!(task_instance_note -> task_instance (ti_id));
diesel::joinable!(task_outlet_asset_reference -> asset (asset_id));
diesel::joinable!(task_outlet_asset_reference -> dag (dag_id));
diesel::joinable!(task_reschedule -> task_instance (ti_id));

diesel::allow_tables_to_appear_in_same_query!(
    alembic_version,
    asset,
    asset_active,
    asset_alias,
    asset_alias_asset,
    asset_alias_asset_event,
    asset_dag_run_queue,
    asset_event,
    asset_trigger,
    backfill,
    backfill_dag_run,
    callback_request,
    connection,
    dag,
    dag_bundle,
    dag_code,
    dag_favorite,
    dag_owner_attributes,
    dag_priority_parsing_request,
    dag_run,
    dag_run_note,
    dag_schedule_asset_alias_reference,
    dag_schedule_asset_name_reference,
    dag_schedule_asset_reference,
    dag_schedule_asset_uri_reference,
    dag_tag,
    dag_version,
    dag_warning,
    dagrun_asset_event,
    deadline,
    import_error,
    job,
    log,
    log_template,
    rendered_task_instance_fields,
    serialized_dag,
    slot_pool,
    task_inlet_asset_reference,
    task_instance,
    task_instance_history,
    task_instance_note,
    task_map,
    task_outlet_asset_reference,
    task_reschedule,
    trigger,
    variable,
    xcom,
);
