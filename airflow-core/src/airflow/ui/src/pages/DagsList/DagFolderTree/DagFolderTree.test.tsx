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

import { BaseWrapper } from "src/utils/Wrapper";

import { DagFolderTree } from "./DagFolderTree";

const FOLDERS = ["team_a/etl", "team_a/report", "team_b/ml"];

describe("DagFolderTree", () => {
  it("renders the top-level folders and an 'All Dags' entry", () => {
    render(<DagFolderTree folders={FOLDERS} onSelectFolder={vi.fn()} selectedFolder={undefined} />, {
      wrapper: BaseWrapper,
    });

    expect(screen.getByText("folders.all")).toBeInTheDocument();
    expect(screen.getByText("team_a")).toBeInTheDocument();
    expect(screen.getByText("team_b")).toBeInTheDocument();
  });

  it("keeps sub-folders collapsed until expanded", () => {
    render(<DagFolderTree folders={FOLDERS} onSelectFolder={vi.fn()} selectedFolder={undefined} />, {
      wrapper: BaseWrapper,
    });

    expect(screen.queryByText("etl")).not.toBeInTheDocument();

    fireEvent.click(screen.getAllByLabelText("Expand")[0] as HTMLElement);

    expect(screen.getByText("etl")).toBeInTheDocument();
    expect(screen.getByText("report")).toBeInTheDocument();
  });

  it("auto-expands the ancestors of the selected folder", () => {
    render(<DagFolderTree folders={FOLDERS} onSelectFolder={vi.fn()} selectedFolder="team_a/etl" />, {
      wrapper: BaseWrapper,
    });

    expect(screen.getByText("etl")).toBeInTheDocument();
  });

  it("calls onSelectFolder with the folder path when a folder is clicked", () => {
    const onSelectFolder = vi.fn();

    render(<DagFolderTree folders={FOLDERS} onSelectFolder={onSelectFolder} selectedFolder={undefined} />, {
      wrapper: BaseWrapper,
    });

    fireEvent.click(screen.getByText("team_b"));

    expect(onSelectFolder).toHaveBeenCalledWith("team_b");
  });

  it("clears the selection when 'All Dags' is clicked", () => {
    const onSelectFolder = vi.fn();

    render(<DagFolderTree folders={FOLDERS} onSelectFolder={onSelectFolder} selectedFolder="team_b/ml" />, {
      wrapper: BaseWrapper,
    });

    fireEvent.click(screen.getByText("folders.all"));

    expect(onSelectFolder).toHaveBeenCalledWith(undefined);
  });

  it("does not select the folder when toggling its expander", () => {
    const onSelectFolder = vi.fn();

    render(<DagFolderTree folders={FOLDERS} onSelectFolder={onSelectFolder} selectedFolder={undefined} />, {
      wrapper: BaseWrapper,
    });

    fireEvent.click(screen.getAllByLabelText("Expand")[0] as HTMLElement);

    expect(onSelectFolder).not.toHaveBeenCalled();
  });

  it("shows an empty message when there are no folders", () => {
    render(<DagFolderTree folders={[]} onSelectFolder={vi.fn()} selectedFolder={undefined} />, {
      wrapper: BaseWrapper,
    });

    expect(screen.getByText("folders.empty")).toBeInTheDocument();
  });
});
