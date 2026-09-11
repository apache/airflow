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
import { useEffect } from "react";
import type { RefObject } from "react";

import "./gridHover.css";

const COL_CLASS = "grid-hover-col";
const ROW_CLASS = "grid-hover-row";

/**
 * Highlight the hovered run column and task row (the crosshair) via direct DOM
 * class toggling instead of React context. A single delegated ``pointerover``
 * listener on ``rootRef`` reads the hovered cell's ``data-run-id`` /
 * ``data-task-id`` and toggles the highlight classes on the matching elements,
 * so moving the mouse across a large grid does zero React render work. The root
 * must contain both the grid and (when present) the gantt so the shared row
 * highlight stays in sync across the two.
 */
export const useGridCrosshairHover = (rootRef: RefObject<HTMLElement | null>) => {
  useEffect(() => {
    const root = rootRef.current;

    if (!root) {
      return undefined;
    }

    let currentRun: string | undefined;
    let currentTask: string | undefined;

    const clear = (className: string) => {
      root.querySelectorAll(`.${className}`).forEach((element) => element.classList.remove(className));
    };

    const apply = (attribute: string, value: string, className: string) => {
      root
        .querySelectorAll(`[${attribute}="${CSS.escape(value)}"]`)
        .forEach((element) => element.classList.add(className));
    };

    const onPointerOver = (event: PointerEvent) => {
      const target = event.target instanceof Element ? event.target : null;
      const runId = target?.closest<HTMLElement>("[data-run-id]")?.dataset.runId;
      const taskId = target?.closest<HTMLElement>("[data-task-id]")?.dataset.taskId;

      if (runId !== currentRun) {
        clear(COL_CLASS);
        if (runId !== undefined) {
          apply("data-run-id", runId, COL_CLASS);
        }
        currentRun = runId;
      }

      if (taskId !== currentTask) {
        clear(ROW_CLASS);
        if (taskId !== undefined) {
          apply("data-task-id", taskId, ROW_CLASS);
        }
        currentTask = taskId;
      }
    };

    const onPointerLeave = () => {
      clear(COL_CLASS);
      clear(ROW_CLASS);
      currentRun = undefined;
      currentTask = undefined;
    };

    root.addEventListener("pointerover", onPointerOver);
    root.addEventListener("pointerleave", onPointerLeave);

    return () => {
      root.removeEventListener("pointerover", onPointerOver);
      root.removeEventListener("pointerleave", onPointerLeave);
      onPointerLeave();
    };
  }, [rootRef]);
};
