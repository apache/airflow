// This file is auto-generated by @hey-api/openapi-ts
import type { CancelablePromise } from "./core/CancelablePromise";
import { OpenAPI } from "./core/OpenAPI";
import { request as __request } from "./core/request";
import type {
  NextRunAssetsData,
  NextRunAssetsResponse,
  GetDagsData,
  GetDagsResponse,
  PatchDagsData,
  PatchDagsResponse,
  GetDagDetailsData,
  GetDagDetailsResponse,
  PatchDagData,
  PatchDagResponse,
  DeleteConnectionData,
  DeleteConnectionResponse,
} from "./types.gen";

export class AssetService {
  /**
   * Next Run Assets
   * @param data The data for the request.
   * @param data.dagId
   * @returns unknown Successful Response
   * @throws ApiError
   */
  public static nextRunAssets(
    data: NextRunAssetsData,
  ): CancelablePromise<NextRunAssetsResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/ui/next_run_datasets/{dag_id}",
      path: {
        dag_id: data.dagId,
      },
      errors: {
        422: "Validation Error",
      },
    });
  }
}

export class DagService {
  /**
   * Get Dags
   * Get all DAGs.
   * @param data The data for the request.
   * @param data.limit
   * @param data.offset
   * @param data.tags
   * @param data.owners
   * @param data.dagIdPattern
   * @param data.dagDisplayNamePattern
   * @param data.onlyActive
   * @param data.paused
   * @param data.lastDagRunState
   * @param data.orderBy
   * @returns DAGCollectionResponse Successful Response
   * @throws ApiError
   */
  public static getDags(
    data: GetDagsData = {},
  ): CancelablePromise<GetDagsResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/dags",
      query: {
        limit: data.limit,
        offset: data.offset,
        tags: data.tags,
        owners: data.owners,
        dag_id_pattern: data.dagIdPattern,
        dag_display_name_pattern: data.dagDisplayNamePattern,
        only_active: data.onlyActive,
        paused: data.paused,
        last_dag_run_state: data.lastDagRunState,
        order_by: data.orderBy,
      },
      errors: {
        422: "Validation Error",
      },
    });
  }

  /**
   * Patch Dags
   * Patch multiple DAGs.
   * @param data The data for the request.
   * @param data.requestBody
   * @param data.updateMask
   * @param data.limit
   * @param data.offset
   * @param data.tags
   * @param data.owners
   * @param data.dagIdPattern
   * @param data.onlyActive
   * @param data.paused
   * @param data.lastDagRunState
   * @returns DAGCollectionResponse Successful Response
   * @throws ApiError
   */
  public static patchDags(
    data: PatchDagsData,
  ): CancelablePromise<PatchDagsResponse> {
    return __request(OpenAPI, {
      method: "PATCH",
      url: "/public/dags",
      query: {
        update_mask: data.updateMask,
        limit: data.limit,
        offset: data.offset,
        tags: data.tags,
        owners: data.owners,
        dag_id_pattern: data.dagIdPattern,
        only_active: data.onlyActive,
        paused: data.paused,
        last_dag_run_state: data.lastDagRunState,
      },
      body: data.requestBody,
      mediaType: "application/json",
      errors: {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }

  /**
   * Get Dag Details
   * Get details of DAG.
   * @param data The data for the request.
   * @param data.dagId
   * @returns DAGDetailsResponse Successful Response
   * @throws ApiError
   */
  public static getDagDetails(
    data: GetDagDetailsData,
  ): CancelablePromise<GetDagDetailsResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/dags/{dag_id}/details",
      path: {
        dag_id: data.dagId,
      },
      errors: {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Unprocessable Entity",
      },
    });
  }

  /**
   * Patch Dag
   * Patch the specific DAG.
   * @param data The data for the request.
   * @param data.dagId
   * @param data.requestBody
   * @param data.updateMask
   * @returns DAGResponse Successful Response
   * @throws ApiError
   */
  public static patchDag(
    data: PatchDagData,
  ): CancelablePromise<PatchDagResponse> {
    return __request(OpenAPI, {
      method: "PATCH",
      url: "/public/dags/{dag_id}",
      path: {
        dag_id: data.dagId,
      },
      query: {
        update_mask: data.updateMask,
      },
      body: data.requestBody,
      mediaType: "application/json",
      errors: {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }
}

export class ConnectionService {
  /**
   * Delete Connection
   * Delete a connection entry.
   * @param data The data for the request.
   * @param data.connectionId
   * @returns void Successful Response
   * @throws ApiError
   */
  public static deleteConnection(
    data: DeleteConnectionData,
  ): CancelablePromise<DeleteConnectionResponse> {
    return __request(OpenAPI, {
      method: "DELETE",
      url: "/public/connections/{connection_id}",
      path: {
        connection_id: data.connectionId,
      },
      errors: {
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }
}
