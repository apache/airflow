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

import { flattenGraph } from "./reactflowUtils";
import type { LayoutNode } from "./useGraphLayout";

describe("flattenGraph", () => {
  it("parents open group children with local positions while keeping edge sections absolute", () => {
    const { edges, nodes } = flattenGraph({
      children: [
        {
          children: [
            {
              height: 80,
              id: "section_1.task_1",
              label: "Task 1",
              type: "task",
              width: 100,
              x: 40,
              y: 60,
            },
          ],
          edges: [
            {
              id: "section_1.task_1-task_2",
              labels: [{ height: 16, id: "label", text: "edge label", width: 80, x: 15, y: 30 }],
              sections: [
                {
                  bendPoints: [{ x: 80, y: 90 }],
                  endPoint: { x: 140, y: 90 },
                  id: "section",
                  startPoint: { x: 40, y: 90 },
                },
              ],
              sources: ["section_1.task_1"],
              targets: ["task_2"],
            },
          ],
          height: 200,
          id: "section_1",
          label: "Section 1",
          type: "task",
          width: 300,
          x: 100,
          y: 200,
        },
      ] as Array<LayoutNode>,
    });

    expect(nodes.find((node) => node.id === "section_1")?.position).toEqual({ x: 100, y: 200 });
    expect(nodes.find((node) => node.id === "section_1.task_1")).toMatchObject({
      parentId: "section_1",
      position: { x: 40, y: 60 },
    });
    expect(edges[0]?.labels?.[0]).toMatchObject({ x: 115, y: 230 });
    expect(edges[0]?.sections?.[0]).toMatchObject({
      bendPoints: [{ x: 180, y: 290 }],
      endPoint: { x: 240, y: 290 },
      startPoint: { x: 140, y: 290 },
    });
  });
});
