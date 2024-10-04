// This file is auto-generated by @hey-api/openapi-ts

/**
 * Connection serializer for responses.
 */
export type ConnectionResponse = {
  conn_id: string;
  conn_type: string;
  description: string | null;
  host: string | null;
  login: string | null;
  schema: string | null;
  port: number | null;
  extra: string | null;
};

/**
 * DAG Collection serializer for responses.
 */
export type DAGCollectionResponse = {
  dags: Array<DAGResponse>;
  total_entries: number;
};

/**
 * Specific serializer for DAG Details responses.
 */
export type DAGDetailsResponse = {
  dag_id: string;
  dag_display_name: string;
  is_paused: boolean;
  is_active: boolean;
  last_parsed_time: string | null;
  last_pickled: string | null;
  last_expired: string | null;
  scheduler_lock: string | null;
  pickle_id: string | null;
  default_view: string | null;
  fileloc: string;
  description: string | null;
  timetable_summary: string | null;
  timetable_description: string | null;
  tags: Array<DagTagPydantic>;
  max_active_tasks: number;
  max_active_runs: number | null;
  max_consecutive_failed_dag_runs: number;
  has_task_concurrency_limits: boolean;
  has_import_errors: boolean;
  next_dagrun: string | null;
  next_dagrun_data_interval_start: string | null;
  next_dagrun_data_interval_end: string | null;
  next_dagrun_create_after: string | null;
  owners: Array<string>;
  catchup: boolean;
  dag_run_timeout: string | null;
  dataset_expression: {
    [key: string]: unknown;
  } | null;
  doc_md: string | null;
  start_date: string | null;
  end_date: string | null;
  is_paused_upon_creation: boolean | null;
  orientation: string;
  params: {
    [key: string]: unknown;
  } | null;
  render_template_as_native_obj: boolean;
  template_search_path: Array<string> | null;
  timezone: string | null;
  last_parsed: string | null;
  /**
   * Return file token.
   */
  readonly file_token: string;
  /**
   * Return max_active_tasks as concurrency.
   */
  readonly concurrency: number;
};

/**
 * Dag Serializer for updatable body.
 */
export type DAGPatchBody = {
  is_paused: boolean;
};

/**
 * DAG serializer for responses.
 */
export type DAGResponse = {
  dag_id: string;
  dag_display_name: string;
  is_paused: boolean;
  is_active: boolean;
  last_parsed_time: string | null;
  last_pickled: string | null;
  last_expired: string | null;
  scheduler_lock: string | null;
  pickle_id: string | null;
  default_view: string | null;
  fileloc: string;
  description: string | null;
  timetable_summary: string | null;
  timetable_description: string | null;
  tags: Array<DagTagPydantic>;
  max_active_tasks: number;
  max_active_runs: number | null;
  max_consecutive_failed_dag_runs: number;
  has_task_concurrency_limits: boolean;
  has_import_errors: boolean;
  next_dagrun: string | null;
  next_dagrun_data_interval_start: string | null;
  next_dagrun_data_interval_end: string | null;
  next_dagrun_create_after: string | null;
  owners: Array<string>;
  /**
   * Return file token.
   */
  readonly file_token: string;
};

/**
 * DAG Run serializer for responses.
 */
export type DAGRunResponse = {
  dag_run_id: string | null;
  dag_id: string;
  logical_date: string | null;
  start_date: string | null;
  end_date: string | null;
  data_interval_start: string | null;
  data_interval_end: string | null;
  last_scheduling_decision: string | null;
  run_type: string;
  state: string;
  external_trigger: boolean;
  conf: {
    [key: string]: unknown;
  };
  notes: string | null;
};

/**
 * All possible states that a DagRun can be in.
 *
 * These are "shared" with TaskInstanceState in some parts of the code,
 * so please ensure that their values always match the ones with the
 * same name in TaskInstanceState.
 */
export type DagRunState = "queued" | "running" | "success" | "failed";

/**
 * Serializable representation of the DagTag ORM SqlAlchemyModel used by internal API.
 */
export type DagTagPydantic = {
  name: string;
  dag_id: string;
};

/**
 * HTTPException Model used for error response.
 */
export type HTTPExceptionResponse = {
  detail:
    | string
    | {
        [key: string]: unknown;
      };
};

export type HTTPValidationError = {
  detail?: Array<ValidationError>;
};

export type ValidationError = {
  loc: Array<string | number>;
  msg: string;
  type: string;
};

export type NextRunAssetsData = {
  dagId: string;
};

export type NextRunAssetsResponse = {
  [key: string]: unknown;
};

export type GetDagsData = {
  dagDisplayNamePattern?: string | null;
  dagIdPattern?: string | null;
  lastDagRunState?: DagRunState | null;
  limit?: number;
  offset?: number;
  onlyActive?: boolean;
  orderBy?: string;
  owners?: Array<string>;
  paused?: boolean | null;
  tags?: Array<string>;
};

export type GetDagsResponse = DAGCollectionResponse;

export type PatchDagsData = {
  dagIdPattern?: string | null;
  lastDagRunState?: DagRunState | null;
  limit?: number;
  offset?: number;
  onlyActive?: boolean;
  owners?: Array<string>;
  paused?: boolean | null;
  requestBody: DAGPatchBody;
  tags?: Array<string>;
  updateMask?: Array<string> | null;
};

export type PatchDagsResponse = DAGCollectionResponse;

export type GetDagDetailsData = {
  dagId: string;
};

export type GetDagDetailsResponse = DAGDetailsResponse;

export type PatchDagData = {
  dagId: string;
  requestBody: DAGPatchBody;
  updateMask?: Array<string> | null;
};

export type PatchDagResponse = DAGResponse;

export type DeleteConnectionData = {
  connectionId: string;
};

export type DeleteConnectionResponse = void;

export type GetConnectionData = {
  connectionId: string;
};

export type GetConnectionResponse = ConnectionResponse;

export type GetDagRunData = {
  dagId: string;
  dagRunId: string;
};

export type GetDagRunResponse = DAGRunResponse;

export type $OpenApiTs = {
  "/ui/next_run_assets/{dag_id}": {
    get: {
      req: NextRunAssetsData;
      res: {
        /**
         * Successful Response
         */
        200: {
          [key: string]: unknown;
        };
        /**
         * Validation Error
         */
        422: HTTPValidationError;
      };
    };
  };
  "/public/dags": {
    get: {
      req: GetDagsData;
      res: {
        /**
         * Successful Response
         */
        200: DAGCollectionResponse;
        /**
         * Validation Error
         */
        422: HTTPValidationError;
      };
    };
    patch: {
      req: PatchDagsData;
      res: {
        /**
         * Successful Response
         */
        200: DAGCollectionResponse;
        /**
         * Bad Request
         */
        400: HTTPExceptionResponse;
        /**
         * Unauthorized
         */
        401: HTTPExceptionResponse;
        /**
         * Forbidden
         */
        403: HTTPExceptionResponse;
        /**
         * Not Found
         */
        404: HTTPExceptionResponse;
        /**
         * Validation Error
         */
        422: HTTPValidationError;
      };
    };
  };
  "/public/dags/{dag_id}/details": {
    get: {
      req: GetDagDetailsData;
      res: {
        /**
         * Successful Response
         */
        200: DAGDetailsResponse;
        /**
         * Bad Request
         */
        400: HTTPExceptionResponse;
        /**
         * Unauthorized
         */
        401: HTTPExceptionResponse;
        /**
         * Forbidden
         */
        403: HTTPExceptionResponse;
        /**
         * Not Found
         */
        404: HTTPExceptionResponse;
        /**
         * Unprocessable Entity
         */
        422: HTTPExceptionResponse;
      };
    };
  };
  "/public/dags/{dag_id}": {
    patch: {
      req: PatchDagData;
      res: {
        /**
         * Successful Response
         */
        200: DAGResponse;
        /**
         * Bad Request
         */
        400: HTTPExceptionResponse;
        /**
         * Unauthorized
         */
        401: HTTPExceptionResponse;
        /**
         * Forbidden
         */
        403: HTTPExceptionResponse;
        /**
         * Not Found
         */
        404: HTTPExceptionResponse;
        /**
         * Validation Error
         */
        422: HTTPValidationError;
      };
    };
  };
  "/public/connections/{connection_id}": {
    delete: {
      req: DeleteConnectionData;
      res: {
        /**
         * Successful Response
         */
        204: void;
        /**
         * Unauthorized
         */
        401: HTTPExceptionResponse;
        /**
         * Forbidden
         */
        403: HTTPExceptionResponse;
        /**
         * Not Found
         */
        404: HTTPExceptionResponse;
        /**
         * Validation Error
         */
        422: HTTPValidationError;
      };
    };
    get: {
      req: GetConnectionData;
      res: {
        /**
         * Successful Response
         */
        200: ConnectionResponse;
        /**
         * Unauthorized
         */
        401: HTTPExceptionResponse;
        /**
         * Forbidden
         */
        403: HTTPExceptionResponse;
        /**
         * Not Found
         */
        404: HTTPExceptionResponse;
        /**
         * Validation Error
         */
        422: HTTPValidationError;
      };
    };
  };
  "/public/dags/{dag_id}/dagRuns/{dag_run_id}": {
    get: {
      req: GetDagRunData;
      res: {
        /**
         * Successful Response
         */
        200: DAGRunResponse;
        /**
         * Unauthorized
         */
        401: HTTPExceptionResponse;
        /**
         * Forbidden
         */
        403: HTTPExceptionResponse;
        /**
         * Not Found
         */
        404: HTTPExceptionResponse;
        /**
         * Validation Error
         */
        422: HTTPValidationError;
      };
    };
  };
};
