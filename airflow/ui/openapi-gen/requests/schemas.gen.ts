// This file is auto-generated by @hey-api/openapi-ts

export const $AppBuilderMenuItemResponse = {
  properties: {
    name: {
      type: "string",
      title: "Name",
    },
    href: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Href",
    },
    category: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Category",
    },
  },
  additionalProperties: true,
  type: "object",
  required: ["name"],
  title: "AppBuilderMenuItemResponse",
  description: "Serializer for AppBuilder Menu Item responses.",
} as const;

export const $AppBuilderViewResponse = {
  properties: {
    name: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Name",
    },
    category: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Category",
    },
    view: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "View",
    },
    label: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Label",
    },
  },
  additionalProperties: true,
  type: "object",
  title: "AppBuilderViewResponse",
  description: "Serializer for AppBuilder View responses.",
} as const;

export const $BaseInfoSchema = {
  properties: {
    status: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Status",
    },
  },
  type: "object",
  required: ["status"],
  title: "BaseInfoSchema",
  description: "Base status field for metadatabase and scheduler.",
} as const;

export const $ConnectionCollectionResponse = {
  properties: {
    connections: {
      items: {
        $ref: "#/components/schemas/ConnectionResponse",
      },
      type: "array",
      title: "Connections",
    },
    total_entries: {
      type: "integer",
      title: "Total Entries",
    },
  },
  type: "object",
  required: ["connections", "total_entries"],
  title: "ConnectionCollectionResponse",
  description: "Connection Collection serializer for responses.",
} as const;

export const $ConnectionResponse = {
  properties: {
    connection_id: {
      type: "string",
      title: "Connection Id",
    },
    conn_type: {
      type: "string",
      title: "Conn Type",
    },
    description: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Description",
    },
    host: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Host",
    },
    login: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Login",
    },
    schema: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Schema",
    },
    port: {
      anyOf: [
        {
          type: "integer",
        },
        {
          type: "null",
        },
      ],
      title: "Port",
    },
    extra: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Extra",
    },
  },
  type: "object",
  required: [
    "connection_id",
    "conn_type",
    "description",
    "host",
    "login",
    "schema",
    "port",
    "extra",
  ],
  title: "ConnectionResponse",
  description: "Connection serializer for responses.",
} as const;

export const $DAGCollectionResponse = {
  properties: {
    dags: {
      items: {
        $ref: "#/components/schemas/DAGResponse",
      },
      type: "array",
      title: "Dags",
    },
    total_entries: {
      type: "integer",
      title: "Total Entries",
    },
  },
  type: "object",
  required: ["dags", "total_entries"],
  title: "DAGCollectionResponse",
  description: "DAG Collection serializer for responses.",
} as const;

export const $DAGDetailsResponse = {
  properties: {
    dag_id: {
      type: "string",
      title: "Dag Id",
    },
    dag_display_name: {
      type: "string",
      title: "Dag Display Name",
    },
    is_paused: {
      type: "boolean",
      title: "Is Paused",
    },
    is_active: {
      type: "boolean",
      title: "Is Active",
    },
    last_parsed_time: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Last Parsed Time",
    },
    last_pickled: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Last Pickled",
    },
    last_expired: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Last Expired",
    },
    scheduler_lock: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Scheduler Lock",
    },
    pickle_id: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Pickle Id",
    },
    default_view: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Default View",
    },
    fileloc: {
      type: "string",
      title: "Fileloc",
    },
    description: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Description",
    },
    timetable_summary: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Timetable Summary",
    },
    timetable_description: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Timetable Description",
    },
    tags: {
      items: {
        $ref: "#/components/schemas/DagTagPydantic",
      },
      type: "array",
      title: "Tags",
    },
    max_active_tasks: {
      type: "integer",
      title: "Max Active Tasks",
    },
    max_active_runs: {
      anyOf: [
        {
          type: "integer",
        },
        {
          type: "null",
        },
      ],
      title: "Max Active Runs",
    },
    max_consecutive_failed_dag_runs: {
      type: "integer",
      title: "Max Consecutive Failed Dag Runs",
    },
    has_task_concurrency_limits: {
      type: "boolean",
      title: "Has Task Concurrency Limits",
    },
    has_import_errors: {
      type: "boolean",
      title: "Has Import Errors",
    },
    next_dagrun: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Next Dagrun",
    },
    next_dagrun_data_interval_start: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Next Dagrun Data Interval Start",
    },
    next_dagrun_data_interval_end: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Next Dagrun Data Interval End",
    },
    next_dagrun_create_after: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Next Dagrun Create After",
    },
    owners: {
      items: {
        type: "string",
      },
      type: "array",
      title: "Owners",
    },
    catchup: {
      type: "boolean",
      title: "Catchup",
    },
    dag_run_timeout: {
      anyOf: [
        {
          type: "string",
          format: "duration",
        },
        {
          type: "null",
        },
      ],
      title: "Dag Run Timeout",
    },
    asset_expression: {
      anyOf: [
        {
          type: "object",
        },
        {
          type: "null",
        },
      ],
      title: "Asset Expression",
    },
    doc_md: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Doc Md",
    },
    start_date: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Start Date",
    },
    end_date: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "End Date",
    },
    is_paused_upon_creation: {
      anyOf: [
        {
          type: "boolean",
        },
        {
          type: "null",
        },
      ],
      title: "Is Paused Upon Creation",
    },
    orientation: {
      type: "string",
      title: "Orientation",
    },
    params: {
      anyOf: [
        {
          type: "object",
        },
        {
          type: "null",
        },
      ],
      title: "Params",
    },
    render_template_as_native_obj: {
      type: "boolean",
      title: "Render Template As Native Obj",
    },
    template_search_path: {
      anyOf: [
        {
          items: {
            type: "string",
          },
          type: "array",
        },
        {
          type: "null",
        },
      ],
      title: "Template Search Path",
    },
    timezone: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Timezone",
    },
    last_parsed: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Last Parsed",
    },
    file_token: {
      type: "string",
      title: "File Token",
      description: "Return file token.",
      readOnly: true,
    },
    concurrency: {
      type: "integer",
      title: "Concurrency",
      description: "Return max_active_tasks as concurrency.",
      readOnly: true,
    },
  },
  type: "object",
  required: [
    "dag_id",
    "dag_display_name",
    "is_paused",
    "is_active",
    "last_parsed_time",
    "last_pickled",
    "last_expired",
    "scheduler_lock",
    "pickle_id",
    "default_view",
    "fileloc",
    "description",
    "timetable_summary",
    "timetable_description",
    "tags",
    "max_active_tasks",
    "max_active_runs",
    "max_consecutive_failed_dag_runs",
    "has_task_concurrency_limits",
    "has_import_errors",
    "next_dagrun",
    "next_dagrun_data_interval_start",
    "next_dagrun_data_interval_end",
    "next_dagrun_create_after",
    "owners",
    "catchup",
    "dag_run_timeout",
    "asset_expression",
    "doc_md",
    "start_date",
    "end_date",
    "is_paused_upon_creation",
    "orientation",
    "params",
    "render_template_as_native_obj",
    "template_search_path",
    "timezone",
    "last_parsed",
    "file_token",
    "concurrency",
  ],
  title: "DAGDetailsResponse",
  description: "Specific serializer for DAG Details responses.",
} as const;

export const $DAGPatchBody = {
  properties: {
    is_paused: {
      type: "boolean",
      title: "Is Paused",
    },
  },
  type: "object",
  required: ["is_paused"],
  title: "DAGPatchBody",
  description: "Dag Serializer for updatable bodies.",
} as const;

export const $DAGResponse = {
  properties: {
    dag_id: {
      type: "string",
      title: "Dag Id",
    },
    dag_display_name: {
      type: "string",
      title: "Dag Display Name",
    },
    is_paused: {
      type: "boolean",
      title: "Is Paused",
    },
    is_active: {
      type: "boolean",
      title: "Is Active",
    },
    last_parsed_time: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Last Parsed Time",
    },
    last_pickled: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Last Pickled",
    },
    last_expired: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Last Expired",
    },
    scheduler_lock: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Scheduler Lock",
    },
    pickle_id: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Pickle Id",
    },
    default_view: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Default View",
    },
    fileloc: {
      type: "string",
      title: "Fileloc",
    },
    description: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Description",
    },
    timetable_summary: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Timetable Summary",
    },
    timetable_description: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Timetable Description",
    },
    tags: {
      items: {
        $ref: "#/components/schemas/DagTagPydantic",
      },
      type: "array",
      title: "Tags",
    },
    max_active_tasks: {
      type: "integer",
      title: "Max Active Tasks",
    },
    max_active_runs: {
      anyOf: [
        {
          type: "integer",
        },
        {
          type: "null",
        },
      ],
      title: "Max Active Runs",
    },
    max_consecutive_failed_dag_runs: {
      type: "integer",
      title: "Max Consecutive Failed Dag Runs",
    },
    has_task_concurrency_limits: {
      type: "boolean",
      title: "Has Task Concurrency Limits",
    },
    has_import_errors: {
      type: "boolean",
      title: "Has Import Errors",
    },
    next_dagrun: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Next Dagrun",
    },
    next_dagrun_data_interval_start: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Next Dagrun Data Interval Start",
    },
    next_dagrun_data_interval_end: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Next Dagrun Data Interval End",
    },
    next_dagrun_create_after: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Next Dagrun Create After",
    },
    owners: {
      items: {
        type: "string",
      },
      type: "array",
      title: "Owners",
    },
    file_token: {
      type: "string",
      title: "File Token",
      description: "Return file token.",
      readOnly: true,
    },
  },
  type: "object",
  required: [
    "dag_id",
    "dag_display_name",
    "is_paused",
    "is_active",
    "last_parsed_time",
    "last_pickled",
    "last_expired",
    "scheduler_lock",
    "pickle_id",
    "default_view",
    "fileloc",
    "description",
    "timetable_summary",
    "timetable_description",
    "tags",
    "max_active_tasks",
    "max_active_runs",
    "max_consecutive_failed_dag_runs",
    "has_task_concurrency_limits",
    "has_import_errors",
    "next_dagrun",
    "next_dagrun_data_interval_start",
    "next_dagrun_data_interval_end",
    "next_dagrun_create_after",
    "owners",
    "file_token",
  ],
  title: "DAGResponse",
  description: "DAG serializer for responses.",
} as const;

export const $DAGRunResponse = {
  properties: {
    run_id: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Run Id",
    },
    dag_id: {
      type: "string",
      title: "Dag Id",
    },
    logical_date: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Logical Date",
    },
    start_date: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Start Date",
    },
    end_date: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "End Date",
    },
    data_interval_start: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Data Interval Start",
    },
    data_interval_end: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Data Interval End",
    },
    last_scheduling_decision: {
      anyOf: [
        {
          type: "string",
          format: "date-time",
        },
        {
          type: "null",
        },
      ],
      title: "Last Scheduling Decision",
    },
    run_type: {
      $ref: "#/components/schemas/DagRunType",
    },
    state: {
      $ref: "#/components/schemas/DagRunState",
    },
    external_trigger: {
      type: "boolean",
      title: "External Trigger",
    },
    triggered_by: {
      $ref: "#/components/schemas/DagRunTriggeredByType",
    },
    conf: {
      type: "object",
      title: "Conf",
    },
    note: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Note",
    },
  },
  type: "object",
  required: [
    "run_id",
    "dag_id",
    "logical_date",
    "start_date",
    "end_date",
    "data_interval_start",
    "data_interval_end",
    "last_scheduling_decision",
    "run_type",
    "state",
    "external_trigger",
    "triggered_by",
    "conf",
    "note",
  ],
  title: "DAGRunResponse",
  description: "DAG Run serializer for responses.",
} as const;

export const $DAGRunStates = {
  properties: {
    queued: {
      type: "integer",
      title: "Queued",
    },
    running: {
      type: "integer",
      title: "Running",
    },
    success: {
      type: "integer",
      title: "Success",
    },
    failed: {
      type: "integer",
      title: "Failed",
    },
  },
  type: "object",
  required: ["queued", "running", "success", "failed"],
  title: "DAGRunStates",
  description: "DAG Run States for responses.",
} as const;

export const $DAGRunTypes = {
  properties: {
    backfill: {
      type: "integer",
      title: "Backfill",
    },
    scheduled: {
      type: "integer",
      title: "Scheduled",
    },
    manual: {
      type: "integer",
      title: "Manual",
    },
    asset_triggered: {
      type: "integer",
      title: "Asset Triggered",
    },
  },
  type: "object",
  required: ["backfill", "scheduled", "manual", "asset_triggered"],
  title: "DAGRunTypes",
  description: "DAG Run Types for responses.",
} as const;

export const $DAGTagCollectionResponse = {
  properties: {
    tags: {
      items: {
        type: "string",
      },
      type: "array",
      title: "Tags",
    },
    total_entries: {
      type: "integer",
      title: "Total Entries",
    },
  },
  type: "object",
  required: ["tags", "total_entries"],
  title: "DAGTagCollectionResponse",
  description: "DAG Tags Collection serializer for responses.",
} as const;

export const $DagProcessorInfoSchema = {
  properties: {
    status: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Status",
    },
    latest_dag_processor_heartbeat: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Latest Dag Processor Heartbeat",
    },
  },
  type: "object",
  required: ["status", "latest_dag_processor_heartbeat"],
  title: "DagProcessorInfoSchema",
  description: "Schema for DagProcessor info.",
} as const;

export const $DagRunState = {
  type: "string",
  enum: ["queued", "running", "success", "failed"],
  title: "DagRunState",
  description: `All possible states that a DagRun can be in.

These are "shared" with TaskInstanceState in some parts of the code,
so please ensure that their values always match the ones with the
same name in TaskInstanceState.`,
} as const;

export const $DagRunTriggeredByType = {
  type: "string",
  enum: [
    "cli",
    "operator",
    "rest_api",
    "ui",
    "test",
    "timetable",
    "dataset",
    "backfill",
  ],
  title: "DagRunTriggeredByType",
  description: "Class with TriggeredBy types for DagRun.",
} as const;

export const $DagRunType = {
  type: "string",
  enum: ["backfill", "scheduled", "manual", "asset_triggered"],
  title: "DagRunType",
  description: "Class with DagRun types.",
} as const;

export const $DagTagPydantic = {
  properties: {
    name: {
      type: "string",
      title: "Name",
    },
    dag_id: {
      type: "string",
      title: "Dag Id",
    },
  },
  type: "object",
  required: ["name", "dag_id"],
  title: "DagTagPydantic",
  description:
    "Serializable representation of the DagTag ORM SqlAlchemyModel used by internal API.",
} as const;

export const $FastAPIAppResponse = {
  properties: {
    app: {
      type: "string",
      title: "App",
    },
    url_prefix: {
      type: "string",
      title: "Url Prefix",
    },
    name: {
      type: "string",
      title: "Name",
    },
  },
  additionalProperties: true,
  type: "object",
  required: ["app", "url_prefix", "name"],
  title: "FastAPIAppResponse",
  description: "Serializer for Plugin FastAPI App responses.",
} as const;

export const $HTTPExceptionResponse = {
  properties: {
    detail: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "object",
        },
      ],
      title: "Detail",
    },
  },
  type: "object",
  required: ["detail"],
  title: "HTTPExceptionResponse",
  description: "HTTPException Model used for error response.",
} as const;

export const $HTTPValidationError = {
  properties: {
    detail: {
      items: {
        $ref: "#/components/schemas/ValidationError",
      },
      type: "array",
      title: "Detail",
    },
  },
  type: "object",
  title: "HTTPValidationError",
} as const;

export const $HealthInfoSchema = {
  properties: {
    metadatabase: {
      $ref: "#/components/schemas/BaseInfoSchema",
    },
    scheduler: {
      $ref: "#/components/schemas/SchedulerInfoSchema",
    },
    triggerer: {
      $ref: "#/components/schemas/TriggererInfoSchema",
    },
    dag_processor: {
      $ref: "#/components/schemas/DagProcessorInfoSchema",
    },
  },
  type: "object",
  required: ["metadatabase", "scheduler", "triggerer", "dag_processor"],
  title: "HealthInfoSchema",
  description: "Schema for the Health endpoint.",
} as const;

export const $HistoricalMetricDataResponse = {
  properties: {
    dag_run_types: {
      $ref: "#/components/schemas/DAGRunTypes",
    },
    dag_run_states: {
      $ref: "#/components/schemas/DAGRunStates",
    },
    task_instance_states: {
      $ref: "#/components/schemas/TaskInstanceState",
    },
  },
  type: "object",
  required: ["dag_run_types", "dag_run_states", "task_instance_states"],
  title: "HistoricalMetricDataResponse",
  description: "Historical Metric Data serializer for responses.",
} as const;

export const $PluginCollectionResponse = {
  properties: {
    plugins: {
      items: {
        $ref: "#/components/schemas/PluginResponse",
      },
      type: "array",
      title: "Plugins",
    },
    total_entries: {
      type: "integer",
      title: "Total Entries",
    },
  },
  type: "object",
  required: ["plugins", "total_entries"],
  title: "PluginCollectionResponse",
  description: "Plugin Collection serializer.",
} as const;

export const $PluginResponse = {
  properties: {
    name: {
      type: "string",
      title: "Name",
    },
    hooks: {
      items: {
        type: "string",
      },
      type: "array",
      title: "Hooks",
    },
    executors: {
      items: {
        type: "string",
      },
      type: "array",
      title: "Executors",
    },
    macros: {
      items: {
        type: "string",
      },
      type: "array",
      title: "Macros",
    },
    flask_blueprints: {
      items: {
        type: "string",
      },
      type: "array",
      title: "Flask Blueprints",
    },
    fastapi_apps: {
      items: {
        $ref: "#/components/schemas/FastAPIAppResponse",
      },
      type: "array",
      title: "Fastapi Apps",
    },
    appbuilder_views: {
      items: {
        $ref: "#/components/schemas/AppBuilderViewResponse",
      },
      type: "array",
      title: "Appbuilder Views",
    },
    appbuilder_menu_items: {
      items: {
        $ref: "#/components/schemas/AppBuilderMenuItemResponse",
      },
      type: "array",
      title: "Appbuilder Menu Items",
    },
    global_operator_extra_links: {
      items: {
        type: "string",
      },
      type: "array",
      title: "Global Operator Extra Links",
    },
    operator_extra_links: {
      items: {
        type: "string",
      },
      type: "array",
      title: "Operator Extra Links",
    },
    source: {
      type: "string",
      title: "Source",
    },
    ti_deps: {
      items: {
        type: "string",
      },
      type: "array",
      title: "Ti Deps",
    },
    listeners: {
      items: {
        type: "string",
      },
      type: "array",
      title: "Listeners",
    },
    timetables: {
      items: {
        type: "string",
      },
      type: "array",
      title: "Timetables",
    },
  },
  type: "object",
  required: [
    "name",
    "hooks",
    "executors",
    "macros",
    "flask_blueprints",
    "fastapi_apps",
    "appbuilder_views",
    "appbuilder_menu_items",
    "global_operator_extra_links",
    "operator_extra_links",
    "source",
    "ti_deps",
    "listeners",
    "timetables",
  ],
  title: "PluginResponse",
  description: "Plugin serializer.",
} as const;

export const $PoolCollectionResponse = {
  properties: {
    pools: {
      items: {
        $ref: "#/components/schemas/PoolResponse",
      },
      type: "array",
      title: "Pools",
    },
    total_entries: {
      type: "integer",
      title: "Total Entries",
    },
  },
  type: "object",
  required: ["pools", "total_entries"],
  title: "PoolCollectionResponse",
  description: "Pool Collection serializer for responses.",
} as const;

export const $PoolResponse = {
  properties: {
    name: {
      type: "string",
      title: "Name",
    },
    slots: {
      type: "integer",
      title: "Slots",
    },
    description: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Description",
    },
    include_deferred: {
      type: "boolean",
      title: "Include Deferred",
    },
    occupied_slots: {
      type: "integer",
      title: "Occupied Slots",
    },
    running_slots: {
      type: "integer",
      title: "Running Slots",
    },
    queued_slots: {
      type: "integer",
      title: "Queued Slots",
    },
    scheduled_slots: {
      type: "integer",
      title: "Scheduled Slots",
    },
    open_slots: {
      type: "integer",
      title: "Open Slots",
    },
    deferred_slots: {
      type: "integer",
      title: "Deferred Slots",
    },
  },
  type: "object",
  required: [
    "name",
    "slots",
    "description",
    "include_deferred",
    "occupied_slots",
    "running_slots",
    "queued_slots",
    "scheduled_slots",
    "open_slots",
    "deferred_slots",
  ],
  title: "PoolResponse",
  description: "Pool serializer for responses.",
} as const;

export const $ProviderCollectionResponse = {
  properties: {
    providers: {
      items: {
        $ref: "#/components/schemas/ProviderResponse",
      },
      type: "array",
      title: "Providers",
    },
    total_entries: {
      type: "integer",
      title: "Total Entries",
    },
  },
  type: "object",
  required: ["providers", "total_entries"],
  title: "ProviderCollectionResponse",
  description: "Provider Collection serializer for responses.",
} as const;

export const $ProviderResponse = {
  properties: {
    package_name: {
      type: "string",
      title: "Package Name",
    },
    description: {
      type: "string",
      title: "Description",
    },
    version: {
      type: "string",
      title: "Version",
    },
  },
  type: "object",
  required: ["package_name", "description", "version"],
  title: "ProviderResponse",
  description: "Provider serializer for responses.",
} as const;

export const $SchedulerInfoSchema = {
  properties: {
    status: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Status",
    },
    latest_scheduler_heartbeat: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Latest Scheduler Heartbeat",
    },
  },
  type: "object",
  required: ["status", "latest_scheduler_heartbeat"],
  title: "SchedulerInfoSchema",
  description: "Schema for Scheduler info.",
} as const;

export const $TaskInstanceState = {
  properties: {
    no_status: {
      type: "integer",
      title: "No Status",
    },
    removed: {
      type: "integer",
      title: "Removed",
    },
    scheduled: {
      type: "integer",
      title: "Scheduled",
    },
    queued: {
      type: "integer",
      title: "Queued",
    },
    running: {
      type: "integer",
      title: "Running",
    },
    success: {
      type: "integer",
      title: "Success",
    },
    restarting: {
      type: "integer",
      title: "Restarting",
    },
    failed: {
      type: "integer",
      title: "Failed",
    },
    up_for_retry: {
      type: "integer",
      title: "Up For Retry",
    },
    up_for_reschedule: {
      type: "integer",
      title: "Up For Reschedule",
    },
    upstream_failed: {
      type: "integer",
      title: "Upstream Failed",
    },
    skipped: {
      type: "integer",
      title: "Skipped",
    },
    deferred: {
      type: "integer",
      title: "Deferred",
    },
  },
  type: "object",
  required: [
    "no_status",
    "removed",
    "scheduled",
    "queued",
    "running",
    "success",
    "restarting",
    "failed",
    "up_for_retry",
    "up_for_reschedule",
    "upstream_failed",
    "skipped",
    "deferred",
  ],
  title: "TaskInstanceState",
  description: "TaskInstance serializer for responses.",
} as const;

export const $TriggererInfoSchema = {
  properties: {
    status: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Status",
    },
    latest_triggerer_heartbeat: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Latest Triggerer Heartbeat",
    },
  },
  type: "object",
  required: ["status", "latest_triggerer_heartbeat"],
  title: "TriggererInfoSchema",
  description: "Schema for Triggerer info.",
} as const;

export const $ValidationError = {
  properties: {
    loc: {
      items: {
        anyOf: [
          {
            type: "string",
          },
          {
            type: "integer",
          },
        ],
      },
      type: "array",
      title: "Location",
    },
    msg: {
      type: "string",
      title: "Message",
    },
    type: {
      type: "string",
      title: "Error Type",
    },
  },
  type: "object",
  required: ["loc", "msg", "type"],
  title: "ValidationError",
} as const;

export const $VariableBody = {
  properties: {
    key: {
      type: "string",
      title: "Key",
    },
    description: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Description",
    },
    value: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Value",
    },
  },
  type: "object",
  required: ["key", "description", "value"],
  title: "VariableBody",
  description: "Variable serializer for bodies.",
} as const;

export const $VariableCollectionResponse = {
  properties: {
    variables: {
      items: {
        $ref: "#/components/schemas/VariableResponse",
      },
      type: "array",
      title: "Variables",
    },
    total_entries: {
      type: "integer",
      title: "Total Entries",
    },
  },
  type: "object",
  required: ["variables", "total_entries"],
  title: "VariableCollectionResponse",
  description: "Variable Collection serializer for responses.",
} as const;

export const $VariableResponse = {
  properties: {
    key: {
      type: "string",
      title: "Key",
    },
    description: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Description",
    },
    value: {
      anyOf: [
        {
          type: "string",
        },
        {
          type: "null",
        },
      ],
      title: "Value",
    },
  },
  type: "object",
  required: ["key", "description", "value"],
  title: "VariableResponse",
  description: "Variable serializer for responses.",
} as const;
