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
import { render } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";

import type { EditorProps } from "src/components/MonacoEditor";

import { Wrapper } from "src/utils/Wrapper";

import { Code } from "./Code";

const source = 'from airflow.sdk import DAG\n\nwith DAG(dag_id="example"):\n    pass\n';

const editorProps = vi.hoisted(() => ({ current: undefined as EditorProps | undefined }));

vi.mock("src/components/MonacoEditor", () => ({
  default: (props: EditorProps) => {
    editorProps.current = props;

    return null;
  },
}));

vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");

  return { ...actual, useParams: () => ({ dagId: "example" }) };
});

vi.mock("openapi/queries", async (importOriginal) => ({
  ...(await importOriginal<typeof OpenapiQueries>()),
  useDagRunServiceGetDagRun: vi.fn(() => ({ data: undefined })),
  useDagServiceGetDagDetails: vi.fn(() => ({ data: undefined, error: null, isLoading: false })),
  useDagSourceServiceGetDagSource: vi.fn(() => ({
    data: { content: source, dag_id: "example", language: "python", version_number: 1 },
    error: null,
    isLoading: false,
  })),
  useDagVersionServiceGetDagVersion: vi.fn(() => ({ data: undefined })),
  useDagVersionServiceGetDagVersions: vi.fn(() => ({ data: { dag_versions: [], total_entries: 0 } })),
}));

vi.mock("src/hooks/useSelectedVersion", () => ({ default: vi.fn(() => undefined) }));
vi.mock("src/queries/useConfig", () => ({ useConfig: vi.fn(() => false) }));

describe("Code", () => {
  it("passes the Dag source, its language and line numbers to the editor", () => {
    render(<Code />, { wrapper: Wrapper });

    expect(editorProps.current?.value).toBe(source);
    expect(editorProps.current?.language).toBe("python");
    expect(editorProps.current?.options?.lineNumbers).toBe("on");
  });
});
