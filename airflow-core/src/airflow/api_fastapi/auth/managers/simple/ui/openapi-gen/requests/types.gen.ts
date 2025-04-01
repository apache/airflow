// This file is auto-generated by @hey-api/openapi-ts

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

/**
 * Login serializer for post bodies.
 */
export type LoginBody = {
  username: string;
  password: string;
};

/**
 * Login serializer for responses.
 */
export type LoginResponse = {
  access_token: string;
};

export type ValidationError = {
  loc: Array<string | number>;
  msg: string;
  type: string;
};

export type CreateTokenAllAdminsResponse = LoginResponse;

export type CreateTokenData = {
  requestBody: LoginBody;
};

export type CreateTokenResponse = LoginResponse;

export type CreateTokenCliData = {
  requestBody: LoginBody;
};

export type CreateTokenCliResponse = LoginResponse;

export type $OpenApiTs = {
  "/auth/token": {
    get: {
      res: {
        /**
         * Successful Response
         */
        201: LoginResponse;
        /**
         * Forbidden
         */
        403: HTTPExceptionResponse;
      };
    };
    post: {
      req: CreateTokenData;
      res: {
        /**
         * Successful Response
         */
        201: LoginResponse;
        /**
         * Bad Request
         */
        400: HTTPExceptionResponse;
        /**
         * Unauthorized
         */
        401: HTTPExceptionResponse;
        /**
         * Validation Error
         */
        422: HTTPValidationError;
      };
    };
  };
  "/auth/token/login": {
    get: {
      res: {
        /**
         * Successful Response
         */
        307: unknown;
        /**
         * Forbidden
         */
        403: HTTPExceptionResponse;
      };
    };
  };
  "/auth/token/cli": {
    post: {
      req: CreateTokenCliData;
      res: {
        /**
         * Successful Response
         */
        201: LoginResponse;
        /**
         * Bad Request
         */
        400: HTTPExceptionResponse;
        /**
         * Unauthorized
         */
        401: HTTPExceptionResponse;
        /**
         * Validation Error
         */
        422: HTTPValidationError;
      };
    };
  };
};
