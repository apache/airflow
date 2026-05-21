/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { useDocumentTitle } from "./useDocumentTitle";

describe("useDocumentTitle", () => {
  const originalTitle = document.title;

  beforeEach(() => {
    document.title = "Airflow";
  });

  afterEach(() => {
    document.title = originalTitle;
  });

  it("sets document title to '{pageTitle} - Airflow' when a page title is provided", () => {
    renderHook(() => useDocumentTitle("my_dag"));
    expect(document.title).toBe("my_dag - Airflow");
  });

  it("sets document title to 'Airflow' when page title is null", () => {
    renderHook(() => useDocumentTitle(null));
    expect(document.title).toBe("Airflow");
  });

  it("sets document title to 'Airflow' when page title is undefined", () => {
    renderHook(() => useDocumentTitle(undefined));
    expect(document.title).toBe("Airflow");
  });

  it("sets document title to 'Airflow' when page title is empty string", () => {
    renderHook(() => useDocumentTitle(""));
    expect(document.title).toBe("Airflow");
  });

  it("resets document title to 'Airflow' on unmount", () => {
    const { unmount } = renderHook(() => useDocumentTitle("my_dag"));
    expect(document.title).toBe("my_dag - Airflow");
    unmount();
    expect(document.title).toBe("Airflow");
  });

  it("updates document title when page title changes", () => {
    const { rerender } = renderHook(({ title }) => useDocumentTitle(title), {
      initialProps: { title: "dag_1" },
    });
    expect(document.title).toBe("dag_1 - Airflow");

    rerender({ title: "dag_2" });
    expect(document.title).toBe("dag_2 - Airflow");
  });

  it("handles complex page titles with separators", () => {
    renderHook(() => useDocumentTitle("my_dag › my_task"));
    expect(document.title).toBe("my_dag › my_task - Airflow");
  });

  it("resets to Airflow when page title changes to null", () => {
    const { rerender } = renderHook(({ title }) => useDocumentTitle(title), {
      initialProps: { title: "my_dag" },
    });
    expect(document.title).toBe("my_dag - Airflow");

    rerender({ title: null });
    expect(document.title).toBe("Airflow");
  });
});
