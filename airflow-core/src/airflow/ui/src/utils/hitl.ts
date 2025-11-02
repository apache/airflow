/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import type { TFunction } from "i18next";

import type { HITLDetail } from "openapi/requests/types.gen";
import type { ParamsSpec } from "src/queries/useDagParams";

export type HITLResponseParams = {
  chosen_options?: Array<string>;
  params_input?: Record<string, unknown>;
};

const getChosenOptionsValue = (hitlDetail: HITLDetail) => {
  // if response_received is true, display the chosen_options, otherwise display the defaults
  const sourceValues = hitlDetail.response_received ? hitlDetail.chosen_options : hitlDetail.defaults;

  return hitlDetail.multiple ? sourceValues : sourceValues?.[0];
};

export const getPreloadHITLFormData = (searchParams: URLSearchParams, hitlDetail: HITLDetail) => {
  const preloadedHITLParams: Record<string, number | string> = Object.fromEntries(
    [...searchParams.entries()]
      .filter(([key]) => key !== "_options")
      .map(([key, value]) => [key, isNaN(Number(value)) ? value : Number(value)]),
  );

  const options = searchParams.get("_options") ?? "";
  let preloadedHITLOptions: Array<string> = [];

  if (options) {
    try {
      const decoded = JSON.parse(decodeURIComponent(options)) as Array<string>;

      preloadedHITLOptions = Array.isArray(decoded) ? decoded : [];
    } catch {
      preloadedHITLOptions = [];
    }
  }

  // Filter the preloaded options to only include the options that are in the hitlDetail.options
  const filteredPreloadedHITLOptions: Array<string> | string | undefined = preloadedHITLOptions.filter(
    (option) => hitlDetail.options.includes(option),
  );

  return {
    preloadedHITLOptions: filteredPreloadedHITLOptions,
    preloadedHITLParams,
  };
};

export const getHITLParamsDict = (
  hitlDetail: HITLDetail,
  translate: TFunction,
  searchParams: URLSearchParams,
): ParamsSpec => {
  const paramsDict: ParamsSpec = {};
  const { preloadedHITLOptions, preloadedHITLParams } = getPreloadHITLFormData(searchParams, hitlDetail);
  const isApprovalTask =
    hitlDetail.options.includes("Approve") &&
    hitlDetail.options.includes("Reject") &&
    hitlDetail.options.length === 2;
  const shouldRenderOptionDropdown = preloadedHITLOptions.length > 0 && !isApprovalTask;

  if (shouldRenderOptionDropdown || hitlDetail.options.length > 4 || hitlDetail.multiple) {
    paramsDict.chosen_options = {
      description: translate("response.optionsDescription"),
      schema: {
        const: undefined,
        description_md: translate("response.optionsDescription"),
        enum: hitlDetail.options.length > 0 ? hitlDetail.options : undefined,
        examples: undefined,
        format: undefined,
        items: hitlDetail.multiple ? { type: "string" } : undefined,
        maximum: undefined,
        maxLength: undefined,
        minimum: undefined,
        minLength: undefined,
        section: undefined,
        title: translate("response.optionsLabel"),
        type: hitlDetail.multiple ? "array" : "string",
        values_display: undefined,
      },

      // If the task is not multiple, we only show the first option
      value:
        getChosenOptionsValue(hitlDetail) ??
        (hitlDetail.multiple ? preloadedHITLOptions : preloadedHITLOptions[0]),
    };
  }

  if (hitlDetail.params) {
    const sourceParams = hitlDetail.response_received ? hitlDetail.params_input : hitlDetail.params;

    Object.entries(sourceParams ?? {}).forEach(([key, value]) => {
      const valueType = typeof value === "number" ? "number" : "string";

      paramsDict[key] = {
        description: "",
        schema: {
          const: undefined,
          description_md: "",
          enum: undefined,
          examples: undefined,
          format: undefined,
          items: undefined,
          maximum: undefined,
          maxLength: undefined,
          minimum: undefined,
          minLength: undefined,
          section: undefined,
          title: key,
          type: valueType,
          values_display: undefined,
        },
        value: preloadedHITLParams[key] ?? value,
      };
    });
  }

  return paramsDict;
};

export const getHITLFormData = (paramsDict: ParamsSpec, option?: string): HITLResponseParams => {
  const chosenOptionsValue = paramsDict.chosen_options?.value;
  let chosenOptions: Array<string> = [];

  if (option === undefined) {
    if (typeof chosenOptionsValue === "string" && chosenOptionsValue) {
      chosenOptions = [chosenOptionsValue];
    } else if (Array.isArray(chosenOptionsValue) && chosenOptionsValue.length > 0) {
      chosenOptions = chosenOptionsValue.filter(
        (value): value is string => value !== null && value !== undefined,
      );
    }
  } else {
    chosenOptions = [option];
  }

  const paramsInput = Object.keys(paramsDict)
    .filter((key) => key !== "chosen_options")
    .reduce<Record<string, unknown>>((acc, key) => {
      acc[key] = paramsDict[key]?.value;

      return acc;
    }, {});

  return {
    chosen_options: chosenOptions,
    params_input: paramsInput,
  };
};

export const getHITLState = (translate: TFunction, hitlDetail: HITLDetail) => {
  const {
    chosen_options: chosenOptions,
    options,
    params,
    response_received: responseReceived,
    task_instance: { state: taskInstanceState },
  } = hitlDetail;

  const isNotDeferred = taskInstanceState !== "deferred";

  let stateType: [string, string] = ["responseRequired", "responseReceived"];

  if (!responseReceived && isNotDeferred) {
    return translate("state.noResponseReceived");
  }

  if (options.length === 2 && options.includes("Approve") && options.includes("Reject")) {
    // If options contain only "Approve" and "Reject" -> approval task
    stateType = [
      "approvalRequired",
      responseReceived && chosenOptions?.includes("Approve") ? "approvalReceived" : "rejectionReceived",
    ];
  } else if (params && Object.keys(params).length === 0) {
    // If it's not an approval task and params are empty -> choice task
    stateType = ["choiceRequired", "choiceReceived"];
  }

  const [required, received] = stateType;

  return translate(`state.${responseReceived ? received : required}`);
};
