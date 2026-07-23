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
import { describe, it, expect } from "vitest";

import { aggregateSlowestTasks, type SlowestTaskInput } from "./slowestTasks";

const ti = (
  taskId: string,
  duration: number | null,
  state: SlowestTaskInput["state"] = "success",
): SlowestTaskInput => ({
  duration,
  state,
  task_display_name: taskId,
  task_id: taskId,
});

describe("aggregateSlowestTasks", () => {
  it("returns an empty list for no instances", () => {
    expect(aggregateSlowestTasks([], 10)).toEqual([]);
  });

  it("computes the median duration per task", () => {
    const result = aggregateSlowestTasks([ti("a", 10), ti("a", 30), ti("a", 20)], 10);

    expect(result).toHaveLength(1);
    expect(result[0]?.medianDuration).toBe(20);
    expect(result[0]?.runCount).toBe(3);
  });

  it("ranks tasks by median duration, slowest first", () => {
    const result = aggregateSlowestTasks([ti("fast", 5), ti("slow", 100), ti("mid", 50)], 10);

    expect(result.map((task) => task.taskId)).toEqual(["slow", "mid", "fast"]);
  });

  it("keeps only the top N", () => {
    const result = aggregateSlowestTasks([ti("a", 40), ti("b", 30), ti("c", 20), ti("d", 10)], 2);

    expect(result.map((task) => task.taskId)).toEqual(["a", "b"]);
  });

  it("ignores instances without a duration", () => {
    const result = aggregateSlowestTasks([ti("a", null), ti("a", 10), ti("a", 20)], 10);

    expect(result[0]?.runCount).toBe(2);
    expect(result[0]?.medianDuration).toBe(15);
  });

  it("drops tasks whose instances all lack a duration", () => {
    expect(aggregateSlowestTasks([ti("running", null)], 10)).toEqual([]);
  });

  it("takes the latest state from the first instance seen (newest-first input)", () => {
    const result = aggregateSlowestTasks([ti("a", 10, "failed"), ti("a", 10, "success")], 10);

    expect(result[0]?.latestState).toBe("failed");
  });
});
