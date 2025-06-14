import type { ApiRequestOptions } from "./ApiRequestOptions";
import type { ApiResult } from "./ApiResult";

export class ApiError extends Error {
  public readonly body: unknown;
  public readonly request: ApiRequestOptions;
  public readonly status: number;
  public readonly statusText: string;
  public readonly url: string;

  constructor(request: ApiRequestOptions, response: ApiResult, message: string) {
    super(message);

    this.name = "ApiError";
    this.url = response.url;
    this.status = response.status;
    this.statusText = response.statusText;
    this.body = response.body;
    this.request = request;
  }
}
