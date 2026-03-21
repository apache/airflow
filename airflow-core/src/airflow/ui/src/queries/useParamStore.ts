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
  // eslint-disable-next-line unicorn/no-null
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
  initialParamDict: ParamsSpec;
  paramsDict: ParamsSpec;
  setConf: (confString: string) => void;
  setDisabled: (disabled: boolean) => void;
  setInitialParamDict: (newParamsDict: ParamsSpec) => void;
  setParamsDict: (newParamsDict: ParamsSpec) => void;
};

const createParamStore = () =>
  create<FormStore>((set) => ({
    conf: "{}",
    disabled: false,
    initialParamDict: {},
    paramsDict: {},

    setConf: (confString: string) =>
      set((state) => {
        if (state.conf === confString) {
          return {};
        }

        const parsedConf = JSON.parse(confString) as Record<string, unknown>;
        const baseDict =
          Object.keys(state.initialParamDict).length > 0 ? state.initialParamDict : state.paramsDict;

        // Preserve a stable ordering of parameters (and thus sections in the trigger form)
        // by following the order from the initial param dict when available.
        const updatedParamsDictEntries: Array<[string, ParamSpec]> = [];
        const inBase = new Set<string>(Object.keys(baseDict));

        for (const [key, baseParam] of Object.entries(baseDict)) {
          if (Object.hasOwn(parsedConf, key)) {
            updatedParamsDictEntries.push([
              key,
              {
                // eslint-disable-next-line unicorn/no-null
                description: baseParam.description ?? null,
                schema: baseParam.schema,
                value: parsedConf[key],
              },
            ]);
          }
        }

        // Append any extra keys that exist in the JSON but not in the base dict.
        for (const [key, value] of Object.entries(parsedConf)) {
          if (!inBase.has(key)) {
            const existingParam = state.paramsDict[key] ?? state.initialParamDict[key];

            updatedParamsDictEntries.push([
              key,
              {
                // eslint-disable-next-line unicorn/no-null
                description: existingParam?.description ?? null,
                schema: existingParam?.schema ?? paramPlaceholder.schema,
                value,
              },
            ]);
          }
        }

        const updatedParamsDict: ParamsSpec = Object.fromEntries(updatedParamsDictEntries);

        return { conf: confString, paramsDict: updatedParamsDict };
      }),

    setDisabled: (disabled: boolean) => set(() => ({ disabled })),

    setInitialParamDict: (newParamsDict: ParamsSpec) => set(() => ({ initialParamDict: newParamsDict })),

    setParamsDict: (newParamsDict: ParamsSpec) =>
      set((state) => {
        const newConf = JSON.stringify(
          Object.fromEntries(Object.entries(newParamsDict).map(([key, { value }]) => [key, value])),
          undefined,
          2,
        );

        if (state.conf === newConf && JSON.stringify(state.paramsDict) === JSON.stringify(newParamsDict)) {
          return {};
        }

        return { conf: newConf, paramsDict: newParamsDict };
      }),
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
