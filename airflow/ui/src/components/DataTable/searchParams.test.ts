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
import { describe, expect, it } from "vitest";

import { searchParamsToState, stateToSearchParams } from "./searchParams";
import type { TableState } from "./types";

describe("searchParams", () => {
  describe("stateToSearchParams", () => {
    it("can serialize table state to search params", () => {
      const state: TableState = {
        pagination: {
          pageIndex: 1,
          pageSize: 20,
        },
        sorting: [{ desc: false, id: "name" }],
      };

      expect(stateToSearchParams(state).toString()).toEqual(
        "limit=20&offset=1&sort=name",
      );
    });
  });
  describe("searchParamsToState", () => {
    it("can parse search params back to table state", () => {
      expect(
        searchParamsToState(
          new URLSearchParams("limit=20&offset=0&sort=name&sort=-age"),
          {
            pagination: {
              pageIndex: 1,
              pageSize: 5,
            },
            sorting: [],
          },
        ),
      ).toEqual({
        pagination: {
          pageIndex: 0,
          pageSize: 20,
        },
        sorting: [
          { desc: false, id: "name" },
          { desc: true, id: "age" },
        ],
      });
    });
  });
});
