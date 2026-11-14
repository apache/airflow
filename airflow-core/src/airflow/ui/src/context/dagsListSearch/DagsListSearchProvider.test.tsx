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
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, useNavigate } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { useDagsListSearch } from "./Context";
import { DagsListSearchProvider } from "./DagsListSearchProvider";

const RememberedSearch = () => <output>{useDagsListSearch()}</output>;

const GoTo = ({ path }: { readonly path: string }) => {
  const navigate = useNavigate();

  return <button onClick={() => void navigate(path)} type="button">{`go ${path}`}</button>;
};

const renderAt = (startPath: string, destinations: Array<string>) =>
  render(
    <MemoryRouter initialEntries={[startPath]}>
      <DagsListSearchProvider>
        <RememberedSearch />
        {destinations.map((path) => (
          <GoTo key={path} path={path} />
        ))}
      </DagsListSearchProvider>
    </MemoryRouter>,
  );

const goTo = (path: string) => fireEvent.click(screen.getByRole("button", { name: `go ${path}` }));

describe("DagsListSearchProvider", () => {
  it("keeps the list's filters and search after leaving it, without paging or sort", () => {
    renderAt("/dags?tags=example&name_pattern=bash&offset=50&limit=25&sort=-dag_id", ["/dags/example_dag"]);

    goTo("/dags/example_dag");

    expect(screen.getByRole("status")).toHaveTextContent("tags=example&name_pattern=bash");
  });

  it("forgets the query once the list is cleared", () => {
    renderAt("/dags?tags=example", ["/dags", "/dags/example_dag"]);

    goTo("/dags");
    goTo("/dags/example_dag");

    expect(screen.getByRole("status")).toBeEmptyDOMElement();
  });

  it("starts empty when the first page is not the list", () => {
    renderAt("/dags/example_dag?tags=example", []);

    expect(screen.getByRole("status")).toBeEmptyDOMElement();
  });
});
