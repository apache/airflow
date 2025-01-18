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

import type { DagParamsSpec, ParamSchema } from "src/queries/useDagParams";

const defaultParamSchema: ParamSchema = {
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
};

type FormStore = {
  conf: string;
  paramsDict: DagParamsSpec;
  setConf: (confString: string) => void;
  setParamsDict: (newParamsDict: DagParamsSpec) => void;
};

export const useParamStore = create<FormStore>((set) => ({
  conf: "{}",
  paramsDict: {},

  setConf: (confString: string) =>
    set((state) => {
      if (state.conf === confString) {
        return {};
      }

      const parsedConf = JSON.parse(confString) as JSON;

      const updatedParamsDict: DagParamsSpec = Object.fromEntries(
        Object.entries(parsedConf).map(([key, value]) => {
          const existingParam = state.paramsDict[key];

          return [
            key,
            {
              description: existingParam?.description,
              schema: existingParam?.schema ?? defaultParamSchema,
              value: value as unknown,
            },
          ];
        }),
      );

      return { conf: confString, paramsDict: updatedParamsDict };
    }),

  setParamsDict: (newParamsDict: DagParamsSpec) =>
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
