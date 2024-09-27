// This file is auto-generated by @hey-api/openapi-ts

/**
 * DAG Collection serializer for responses.
 */
export type DAGCollectionResponse = {
  dags: Array<DAGResponse>;
  total_entries: number;
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
 * Serializable representation of the DagTag ORM SqlAlchemyModel used by internal API.
 */
export type DagTagPydantic = {
  name: string;
  dag_id: string;
};

export type HTTPValidationError = {
  detail?: Array<ValidationError>;
};

export type ValidationError = {
  loc: Array<string | number>;
  msg: string;
  type: string;
};

export type NextRunDatasetsUiNextRunDatasetsDagIdGetData = {
  dagId: string;
};

export type NextRunDatasetsUiNextRunDatasetsDagIdGetResponse = {
  [key: string]: unknown;
};

export type GetDagsPublicDagsGetData = {
  dagDisplayNamePattern?: string | null;
  dagIdPattern?: string | null;
  limit?: number;
  offset?: number;
  onlyActive?: boolean;
  orderBy?: string;
  owners?: Array<string>;
  paused?: boolean | null;
  tags?: Array<string>;
};

export type GetDagsPublicDagsGetResponse = DAGCollectionResponse;

export type PatchDagPublicDagsDagIdPatchData = {
  dagId: string;
  requestBody: DAGPatchBody;
  updateMask?: Array<string> | null;
};

export type PatchDagPublicDagsDagIdPatchResponse = DAGResponse;

export type $OpenApiTs = {
  "/ui/next_run_datasets/{dag_id}": {
    get: {
      req: NextRunDatasetsUiNextRunDatasetsDagIdGetData;
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
      req: GetDagsPublicDagsGetData;
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
  };
  "/public/dags/{dag_id}": {
    patch: {
      req: PatchDagPublicDagsDagIdPatchData;
      res: {
        /**
         * Successful Response
         */
        200: DAGResponse;
        /**
         * Validation Error
         */
        422: HTTPValidationError;
      };
    };
  };
};
