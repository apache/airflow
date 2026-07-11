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
  initialParamDict: ParamsSpec;
  initParamsDictFromConf: (newParamsDict: ParamsSpec) => void;
  mergeParamUpdatesIntoConf: boolean;
  paramsDict: ParamsSpec;
  paramsDictSnapshot: ParamsSpec;
  setConf: (confString: string) => void;
  setDisabled: (disabled: boolean) => void;
  setInitialParamDict: (newParamsDict: ParamsSpec) => void;
  setMergeParamUpdatesIntoConf: (mergeParamUpdatesIntoConf: boolean) => void;
  setParamsDict: (newParamsDict: ParamsSpec) => void;
};

const valuesAreEqual = (left: unknown, right: unknown) => JSON.stringify(left) === JSON.stringify(right);

const hasValue = (value: unknown) => value !== "" && value !== null && value !== undefined;

const parseConfObject = (confString: string) => {
  if (confString.trim() === "") {
    return {};
  }

  const parsedConf = JSON.parse(confString) as unknown;

  return typeof parsedConf === "object" && parsedConf !== null && !Array.isArray(parsedConf)
    ? (parsedConf as Record<string, unknown>)
    : {};
};

const buildParamsDictFromConf = ({
  baseDict,
  confString,
  includeMissingParams,
  includeUnknownParams,
  paramsDict,
}: {
  readonly baseDict: ParamsSpec;
  readonly confString: string;
  readonly includeMissingParams: boolean;
  readonly includeUnknownParams: boolean;
  readonly paramsDict: ParamsSpec;
}) => {
  const parsedConf = parseConfObject(confString);

  // Preserve a stable ordering of parameters (and thus sections in the trigger form)
  // by following the order from the initial param dict when available.
  const updatedParamsDictEntries: Array<[string, ParamSpec]> = [];
  const inBase = new Set<string>(Object.keys(baseDict));

  for (const [key, baseParam] of Object.entries(baseDict)) {
    if (Object.hasOwn(parsedConf, key) || includeMissingParams) {
      updatedParamsDictEntries.push([
        key,
        {
          description: baseParam.description ?? null,
          schema: baseParam.schema,
          value: Object.hasOwn(parsedConf, key) ? parsedConf[key] : undefined,
        },
      ]);
    }
  }

  if (includeUnknownParams) {
    // Append any extra keys that exist in the JSON but not in the base dict.
    for (const [key, value] of Object.entries(parsedConf)) {
      if (!inBase.has(key)) {
        const existingParam = paramsDict[key] ?? baseDict[key];

        updatedParamsDictEntries.push([
          key,
          {
            description: existingParam?.description ?? null,
            schema: existingParam?.schema ?? paramPlaceholder.schema,
            value,
          },
        ]);
      }
    }
  }

  return Object.fromEntries(updatedParamsDictEntries);
};

const createParamStore = () =>
  create<FormStore>((set) => ({
    conf: "{}",
    disabled: false,
    initialParamDict: {},
    initParamsDictFromConf: (newParamsDict: ParamsSpec) =>
      set((state) => {
        const paramsDict = buildParamsDictFromConf({
          baseDict: newParamsDict,
          confString: state.conf,
          includeMissingParams: true,
          includeUnknownParams: false,
          paramsDict: state.paramsDict,
        });

        return {
          initialParamDict: newParamsDict,
          paramsDict,
          paramsDictSnapshot: structuredClone(paramsDict),
        };
      }),
    mergeParamUpdatesIntoConf: false,
    paramsDict: {},
    paramsDictSnapshot: {},

    setConf: (confString: string) =>
      set((state) => {
        if (state.conf === confString) {
          return {};
        }

        const baseDict =
          Object.keys(state.initialParamDict).length > 0 ? state.initialParamDict : state.paramsDict;
        const paramsDict = buildParamsDictFromConf({
          baseDict,
          confString,
          includeMissingParams: state.mergeParamUpdatesIntoConf,
          includeUnknownParams: !state.mergeParamUpdatesIntoConf,
          paramsDict: state.paramsDict,
        });

        return {
          conf: confString,
          paramsDict,
          paramsDictSnapshot: state.mergeParamUpdatesIntoConf ? structuredClone(paramsDict) : {},
        };
      }),

    setDisabled: (disabled: boolean) => set(() => ({ disabled })),

    setInitialParamDict: (newParamsDict: ParamsSpec) => set(() => ({ initialParamDict: newParamsDict })),

    setMergeParamUpdatesIntoConf: (mergeParamUpdatesIntoConf: boolean) =>
      set(() => ({ mergeParamUpdatesIntoConf })),

    setParamsDict: (newParamsDict: ParamsSpec) =>
      set((state) => {
        if (state.mergeParamUpdatesIntoConf) {
          const changedKeys = Object.entries(newParamsDict)
            .filter(([key, param]) => !valuesAreEqual(param.value, state.paramsDictSnapshot[key]?.value))
            .map(([key]) => key);

          if (changedKeys.length === 0) {
            return { paramsDict: newParamsDict, paramsDictSnapshot: structuredClone(newParamsDict) };
          }

          let parsedConf: Record<string, unknown>;

          try {
            parsedConf = parseConfObject(state.conf);
          } catch {
            return { paramsDict: newParamsDict, paramsDictSnapshot: structuredClone(newParamsDict) };
          }

          for (const key of changedKeys) {
            const value = newParamsDict[key]?.value;

            if (hasValue(value)) {
              parsedConf[key] = value;
            } else {
              parsedConf = Object.fromEntries(
                Object.entries(parsedConf).filter(([existingKey]) => existingKey !== key),
              );
            }
          }

          return {
            conf: JSON.stringify(parsedConf, undefined, 2),
            paramsDict: newParamsDict,
            paramsDictSnapshot: structuredClone(newParamsDict),
          };
        }

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
