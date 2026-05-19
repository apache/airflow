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
import { create } from "zustand";

import type { ParamsSpec, ParamSpec } from "src/queries/useDagParams";

export const paramPlaceholder: ParamSpec = {
  description: null,
  schema: {
    const: undefined,
    description_md: undefined,
    enum: undefined,
    examples: undefined,
    format: undefined,
    items: undefined,
    maximum: undefined,
    maxLength: undefined,
    minimum: undefined,
    minLength: undefined,
    section: undefined,
    title: undefined,
    type: undefined,
    values_display: undefined,
  },
  value: "",
};

type FormStore = {
  conf: string;
  disabled: boolean;
  initializeParamsDict: (newParamsDict: ParamsSpec) => void;
  initialParamDict: ParamsSpec;
  paramsDict: ParamsSpec;
  setConf: (confString: string) => void;
  setDisabled: (disabled: boolean) => void;
  setInitialParamDict: (newParamsDict: ParamsSpec) => void;
  setParamsDict: (newParamsDict: ParamsSpec, touchedKey?: string) => void;
  touchedKeys: ReadonlySet<string>;
};

export const hydrateParams = (newParamsDict: ParamsSpec, confString: string): ParamsSpec => {
  const parsedConf = JSON.parse(confString) as Record<string, unknown>;
  const paramsWithValues: ParamsSpec = {};

  for (const [key, param] of Object.entries(newParamsDict)) {
    paramsWithValues[key] = {
      description: param.description,
      schema: param.schema,
      value: Object.hasOwn(parsedConf, key) ? parsedConf[key] : param.value,
    };
  }

  for (const [key, value] of Object.entries(parsedConf)) {
    if (!Object.hasOwn(newParamsDict, key)) {
      paramsWithValues[key] = {
        description: null,
        schema: paramPlaceholder.schema,
        value,
      };
    }
  }

  return paramsWithValues;
};

export const serializeConf = (
  paramsDict: ParamsSpec,
  confString: string,
  touchedKeys: ReadonlySet<string>,
) => {
  const nextConf = JSON.parse(confString) as Record<string, unknown>;

  for (const key of touchedKeys) {
    const param = paramsDict[key];

    if (param !== undefined) {
      nextConf[key] = param.value;
    }
  }

  return JSON.stringify(nextConf, undefined, 2);
};

const createParamStore = () =>
  create<FormStore>((set) => ({
    conf: "{}",
    disabled: false,
    initializeParamsDict: (newParamsDict: ParamsSpec) =>
      set((state) => ({
        paramsDict: hydrateParams(newParamsDict, state.conf),
      })),
    initialParamDict: {},
    paramsDict: {},

    setConf: (confString: string) =>
      set((state) => {
        if (state.conf === confString) {
          return {};
        }

        const baseDict =
          Object.keys(state.initialParamDict).length > 0 ? state.initialParamDict : state.paramsDict;
        const paramsDict =
          Object.keys(baseDict).length > 0 ? hydrateParams(baseDict, confString) : state.paramsDict;

        return { conf: confString, paramsDict, touchedKeys: new Set<string>() };
      }),

    setDisabled: (disabled: boolean) => set(() => ({ disabled })),

    setInitialParamDict: (newParamsDict: ParamsSpec) => set(() => ({ initialParamDict: newParamsDict })),

    setParamsDict: (newParamsDict: ParamsSpec, touchedKey?: string) =>
      set((state) => {
        if (Object.keys(newParamsDict).length === 0) {
          return { conf: "{}", paramsDict: {}, touchedKeys: new Set<string>() };
        }

        const touchedKeys = new Set(state.touchedKeys);

        if (touchedKey !== undefined) {
          touchedKeys.add(touchedKey);
        }

        const newConf = serializeConf(newParamsDict, state.conf, touchedKeys);

        if (
          state.conf === newConf &&
          JSON.stringify(state.paramsDict) === JSON.stringify(newParamsDict) &&
          touchedKeys.size === state.touchedKeys.size
        ) {
          return {};
        }

        return { conf: newConf, paramsDict: newParamsDict, touchedKeys };
      }),
    touchedKeys: new Set<string>(),
  }));

const stores = new Map<string, ReturnType<typeof createParamStore>>();

export const useParamStore = (namespace = "default") => {
  if (!stores.has(namespace)) {
    stores.set(namespace, createParamStore());
  }

  const store = stores.get(namespace);

  if (!store) {
    throw new Error(`Failed to create store for namespace: ${namespace}`);
  }

  return store();
};
