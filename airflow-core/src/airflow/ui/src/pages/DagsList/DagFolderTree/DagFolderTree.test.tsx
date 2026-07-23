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
import "@testing-library/jest-dom";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { DagFolderResponse } from "openapi/requests/types.gen";
import { BaseWrapper } from "src/utils/Wrapper";

import { DagFolderTree } from "./DagFolderTree";

const SINGLE_BUNDLE: Array<DagFolderResponse> = [
  { bundle_name: "dags-folder", folder: "team_a/etl" },
  { bundle_name: "dags-folder", folder: "team_a/report" },
  { bundle_name: "dags-folder", folder: "team_b/ml" },
];

// Both bundles reuse the ``team_a/etl`` path to check they stay separate.
const MULTI_BUNDLE: Array<DagFolderResponse> = [
  { bundle_name: "analytics", folder: "team_a/etl" },
  { bundle_name: "ml", folder: "team_a/etl" },
  { bundle_name: "ml", folder: "features" },
];

describe("DagFolderTree (single bundle)", () => {
  it("renders the top-level folders and an 'All Dags' entry, without a bundle level", () => {
    render(
      <DagFolderTree
        folders={SINGLE_BUNDLE}
        onSelectFolder={vi.fn()}
        selectedBundle={undefined}
        selectedFolder={undefined}
      />,
      { wrapper: BaseWrapper },
    );

    expect(screen.getByText("folders.all")).toBeInTheDocument();
    expect(screen.getByText("team_a")).toBeInTheDocument();
    expect(screen.getByText("team_b")).toBeInTheDocument();
    expect(screen.queryByText("dags-folder")).not.toBeInTheDocument();
  });

  it("keeps sub-folders collapsed until expanded", () => {
    render(
      <DagFolderTree
        folders={SINGLE_BUNDLE}
        onSelectFolder={vi.fn()}
        selectedBundle={undefined}
        selectedFolder={undefined}
      />,
      { wrapper: BaseWrapper },
    );

    expect(screen.queryByText("etl")).not.toBeInTheDocument();

    fireEvent.click(screen.getAllByLabelText("Expand")[0] as HTMLElement);

    expect(screen.getByText("etl")).toBeInTheDocument();
    expect(screen.getByText("report")).toBeInTheDocument();
  });

  it("auto-expands the ancestors of the selected folder", () => {
    render(
      <DagFolderTree
        folders={SINGLE_BUNDLE}
        onSelectFolder={vi.fn()}
        selectedBundle={undefined}
        selectedFolder="team_a/etl"
      />,
      { wrapper: BaseWrapper },
    );

    expect(screen.getByText("etl")).toBeInTheDocument();
  });

  it("calls onSelectFolder with the folder path (no bundle) when a folder is clicked", () => {
    const onSelectFolder = vi.fn();

    render(
      <DagFolderTree
        folders={SINGLE_BUNDLE}
        onSelectFolder={onSelectFolder}
        selectedBundle={undefined}
        selectedFolder={undefined}
      />,
      { wrapper: BaseWrapper },
    );

    fireEvent.click(screen.getByText("team_b"));

    expect(onSelectFolder).toHaveBeenCalledWith({ bundleName: undefined, folder: "team_b" });
  });

  it("clears the selection when 'All Dags' is clicked", () => {
    const onSelectFolder = vi.fn();

    render(
      <DagFolderTree
        folders={SINGLE_BUNDLE}
        onSelectFolder={onSelectFolder}
        selectedBundle={undefined}
        selectedFolder="team_b/ml"
      />,
      { wrapper: BaseWrapper },
    );

    fireEvent.click(screen.getByText("folders.all"));

    expect(onSelectFolder).toHaveBeenCalledWith({ bundleName: undefined, folder: undefined });
  });

  it("does not select the folder when toggling its expander", () => {
    const onSelectFolder = vi.fn();

    render(
      <DagFolderTree
        folders={SINGLE_BUNDLE}
        onSelectFolder={onSelectFolder}
        selectedBundle={undefined}
        selectedFolder={undefined}
      />,
      { wrapper: BaseWrapper },
    );

    fireEvent.click(screen.getAllByLabelText("Expand")[0] as HTMLElement);

    expect(onSelectFolder).not.toHaveBeenCalled();
  });

  it("shows an empty message when there are no folders", () => {
    render(
      <DagFolderTree
        folders={[]}
        onSelectFolder={vi.fn()}
        selectedBundle={undefined}
        selectedFolder={undefined}
      />,
      { wrapper: BaseWrapper },
    );

    expect(screen.getByText("folders.empty")).toBeInTheDocument();
  });
});

describe("DagFolderTree (multiple bundles)", () => {
  it("renders bundles as top-level entries", () => {
    render(
      <DagFolderTree
        folders={MULTI_BUNDLE}
        onSelectFolder={vi.fn()}
        selectedBundle={undefined}
        selectedFolder={undefined}
      />,
      { wrapper: BaseWrapper },
    );

    expect(screen.getByText("analytics")).toBeInTheDocument();
    expect(screen.getByText("ml")).toBeInTheDocument();
    // Folders stay hidden until a bundle is expanded.
    expect(screen.queryByText("features")).not.toBeInTheDocument();
  });

  it("calls onSelectFolder with the bundle (no folder) when a bundle is clicked", () => {
    const onSelectFolder = vi.fn();

    render(
      <DagFolderTree
        folders={MULTI_BUNDLE}
        onSelectFolder={onSelectFolder}
        selectedBundle={undefined}
        selectedFolder={undefined}
      />,
      { wrapper: BaseWrapper },
    );

    fireEvent.click(screen.getByText("analytics"));

    expect(onSelectFolder).toHaveBeenCalledWith({ bundleName: "analytics", folder: undefined });
  });

  it("keeps the same folder path separate under each bundle", () => {
    const onSelectFolder = vi.fn();

    render(
      <DagFolderTree
        folders={MULTI_BUNDLE}
        onSelectFolder={onSelectFolder}
        selectedBundle={undefined}
        selectedFolder={undefined}
      />,
      { wrapper: BaseWrapper },
    );

    // Expand the "ml" bundle (second one alphabetically) and select its leaf folder.
    fireEvent.click(screen.getAllByLabelText("Expand")[1] as HTMLElement);
    fireEvent.click(screen.getByText("features"));

    expect(onSelectFolder).toHaveBeenCalledWith({ bundleName: "ml", folder: "features" });
  });

  it("auto-expands the selected bundle down to the selected folder", () => {
    render(
      <DagFolderTree
        folders={MULTI_BUNDLE}
        onSelectFolder={vi.fn()}
        selectedBundle="ml"
        selectedFolder="team_a/etl"
      />,
      { wrapper: BaseWrapper },
    );

    expect(screen.getByText("etl")).toBeInTheDocument();
  });
});
