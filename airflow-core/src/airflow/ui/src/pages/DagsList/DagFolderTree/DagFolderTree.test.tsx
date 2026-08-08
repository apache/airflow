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

// Tree nodes are identified by their own label, ignoring the labels of nested children.
// ``hidden`` is needed because collapsed branches are hidden from the accessibility tree.
const nodes = () => screen.getAllByRole("treeitem", { hidden: true });

const node = (label: string) =>
  nodes().find(
    (item) => item.querySelector('[data-part="branch-text"], [data-part="item-text"]')?.textContent === label,
  );

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

    expect(screen.getByRole("button", { name: "folders.all" })).toBeInTheDocument();
    expect(node("team_a")).toBeDefined();
    expect(node("team_b")).toBeDefined();
    expect(screen.queryByText("dags-folder")).not.toBeInTheDocument();
  });

  it("renders an accessible tree instead of click handlers on plain elements", () => {
    render(
      <DagFolderTree
        folders={SINGLE_BUNDLE}
        onSelectFolder={vi.fn()}
        selectedBundle={undefined}
        selectedFolder={undefined}
      />,
      { wrapper: BaseWrapper },
    );

    expect(screen.getByRole("tree")).toBeInTheDocument();
    expect(nodes().length).toBeGreaterThan(0);
    // Expandable nodes expose their state, and the tree is keyboard reachable.
    expect(node("team_a")).toHaveAttribute("aria-expanded", "false");
    expect(node("team_a")).toHaveAttribute("aria-level", "1");
    expect(screen.getAllByRole("button").length).toBeGreaterThan(0);
  });

  it("reserves the expand indicator width on leaves so siblings line up", () => {
    render(
      <DagFolderTree
        folders={[
          { bundle_name: "dags-folder", folder: "reports" },
          { bundle_name: "dags-folder", folder: "team_a/etl" },
        ]}
        onSelectFolder={vi.fn()}
        selectedBundle={undefined}
        selectedFolder={undefined}
      />,
      { wrapper: BaseWrapper },
    );

    // ``reports`` has no children, so it renders a hidden placeholder where ``team_a`` shows
    // its expand indicator; without it the two siblings would not be indented the same way.
    const leaf = node("reports");

    expect(leaf?.querySelector('[aria-hidden="true"]')).toBeInTheDocument();
    expect(node("team_a")?.querySelector('[data-part="branch-indicator"]')).toBeInTheDocument();
  });

  it("keeps sub-folders hidden until their parent is expanded", () => {
    render(
      <DagFolderTree
        folders={SINGLE_BUNDLE}
        onSelectFolder={vi.fn()}
        selectedBundle={undefined}
        selectedFolder={undefined}
      />,
      { wrapper: BaseWrapper },
    );

    expect(screen.getByText("etl")).not.toBeVisible();
  });

  it("expands the ancestors of the selected folder and marks it as selected", () => {
    render(
      <DagFolderTree
        folders={SINGLE_BUNDLE}
        onSelectFolder={vi.fn()}
        selectedBundle={undefined}
        selectedFolder="team_a/etl"
      />,
      { wrapper: BaseWrapper },
    );

    expect(node("team_a")).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByText("etl")).toBeVisible();
    expect(node("etl")).toHaveAttribute("aria-selected", "true");
    expect(node("team_b")).toHaveAttribute("aria-selected", "false");
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

    fireEvent.click(screen.getByRole("button", { name: "folders.all" }));

    expect(onSelectFolder).toHaveBeenCalledWith({ bundleName: undefined, folder: undefined });
  });

  it("shows an empty message and no tree when there are no folders", () => {
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
    expect(screen.queryByRole("tree")).not.toBeInTheDocument();
  });

  it("renders skeletons while loading", () => {
    render(
      <DagFolderTree
        folders={[]}
        isLoading
        onSelectFolder={vi.fn()}
        selectedBundle={undefined}
        selectedFolder={undefined}
      />,
      { wrapper: BaseWrapper },
    );

    expect(screen.queryByRole("tree")).not.toBeInTheDocument();
    expect(screen.queryByText("folders.empty")).not.toBeInTheDocument();
  });
});

describe("DagFolderTree (multiple bundles)", () => {
  it("renders bundles as the top level with their folders nested underneath", () => {
    render(
      <DagFolderTree
        folders={MULTI_BUNDLE}
        onSelectFolder={vi.fn()}
        selectedBundle={undefined}
        selectedFolder={undefined}
      />,
      { wrapper: BaseWrapper },
    );

    expect(node("analytics")).toHaveAttribute("aria-level", "1");
    expect(node("ml")).toHaveAttribute("aria-level", "1");
    // Folders live below their bundle and stay hidden until it is expanded.
    expect(screen.getByText("features")).not.toBeVisible();
    expect(node("features")).toHaveAttribute("aria-level", "2");
  });

  it("keeps the same folder path separate under each bundle", () => {
    render(
      <DagFolderTree
        folders={MULTI_BUNDLE}
        onSelectFolder={vi.fn()}
        selectedBundle={undefined}
        selectedFolder={undefined}
      />,
      { wrapper: BaseWrapper },
    );

    // ``team_a`` exists in both bundles, so it must appear once per bundle.
    const teamANodes = nodes().filter(
      (item) => item.querySelector('[data-part="branch-text"]')?.textContent === "team_a",
    );

    expect(teamANodes).toHaveLength(2);
    expect(teamANodes.map((item) => item.getAttribute("data-value"))).toEqual([
      "folder:analytics:team_a",
      "folder:ml:team_a",
    ]);
  });

  it("expands the selected bundle down to the selected folder", () => {
    render(
      <DagFolderTree
        folders={MULTI_BUNDLE}
        onSelectFolder={vi.fn()}
        selectedBundle="ml"
        selectedFolder="team_a/etl"
      />,
      { wrapper: BaseWrapper },
    );

    expect(node("ml")).toHaveAttribute("aria-expanded", "true");

    // ``etl`` exists in both bundles; only the one under ``ml`` is selected.
    const selected = nodes().filter((item) => item.getAttribute("aria-selected") === "true");

    expect(selected.map((item) => item.getAttribute("data-value"))).toEqual(["folder:ml:team_a/etl"]);
  });

  it("marks a bundle as selected when only the bundle is selected", () => {
    render(
      <DagFolderTree
        folders={MULTI_BUNDLE}
        onSelectFolder={vi.fn()}
        selectedBundle="analytics"
        selectedFolder={undefined}
      />,
      { wrapper: BaseWrapper },
    );

    expect(node("analytics")).toHaveAttribute("aria-selected", "true");
    expect(node("ml")).toHaveAttribute("aria-selected", "false");
  });
});
