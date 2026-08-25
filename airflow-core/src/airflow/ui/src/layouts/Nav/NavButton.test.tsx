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
import { render, screen } from "@testing-library/react";
import type { PropsWithChildren } from "react";
import { FiHome } from "react-icons/fi";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { NavButton } from "./NavButton";

const wrapperAt = (path: string) => {
  const wrapper = ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={[path]}>{children}</MemoryRouter>
    </BaseWrapper>
  );

  return wrapper;
};

describe("NavButton", () => {
  describe("single `to`", () => {
    it("renders as a link to that destination", () => {
      render(<NavButton icon={FiHome} title="Dags" to="dags" />, { wrapper: wrapperAt("/") });

      expect(screen.getByRole("link", { name: "Dags" })).toHaveAttribute("href", "/dags");
    });

    it("is active when the current route matches", () => {
      render(<NavButton icon={FiHome} title="Dags" to="dags" />, { wrapper: wrapperAt("/dags") });

      expect(screen.getByRole("link", { name: "Dags" })).toHaveAttribute("aria-current", "page");
    });

    it("is active on a nested route under the destination", () => {
      render(<NavButton icon={FiHome} title="Dags" to="dags" />, {
        wrapper: wrapperAt("/dags/my_dag/runs"),
      });

      expect(screen.getByRole("link", { name: "Dags" })).toHaveAttribute("aria-current", "page");
    });

    it("is not active on an unrelated route", () => {
      render(<NavButton icon={FiHome} title="Dags" to="dags" />, { wrapper: wrapperAt("/assets") });

      expect(screen.getByRole("link", { name: "Dags" })).not.toHaveAttribute("aria-current");
    });
  });

  describe("multiple `to`", () => {
    it("renders as a plain button, not a link", () => {
      render(<NavButton icon={FiHome} title="Browse" to={["events", "jobs"]} />, {
        wrapper: wrapperAt("/"),
      });

      const button = screen.getByRole("button", { name: "Browse" });

      expect(button).not.toHaveAttribute("href");
    });

    it("is active when the current route matches any of the destinations", () => {
      render(<NavButton icon={FiHome} title="Browse" to={["events", "jobs"]} />, {
        wrapper: wrapperAt("/jobs"),
      });

      expect(screen.getByRole("button", { name: "Browse" })).toHaveAttribute("aria-current", "page");
    });

    it("is not active when the current route matches none of the destinations", () => {
      render(<NavButton icon={FiHome} title="Browse" to={["events", "jobs"]} />, {
        wrapper: wrapperAt("/xcoms"),
      });

      expect(screen.getByRole("button", { name: "Browse" })).not.toHaveAttribute("aria-current");
    });
  });

  describe("matchPaths", () => {
    it("is active on an extra match path even though `to` points elsewhere", () => {
      render(<NavButton icon={FiHome} matchPaths={["dag_runs", "task_instances"]} title="Dags" to="dags" />, {
        wrapper: wrapperAt("/dag_runs"),
      });

      expect(screen.getByRole("link", { name: "Dags" })).toHaveAttribute("aria-current", "page");
    });

    it("is not active on a route outside both `to` and matchPaths", () => {
      render(<NavButton icon={FiHome} matchPaths={["dag_runs", "task_instances"]} title="Dags" to="dags" />, {
        wrapper: wrapperAt("/assets"),
      });

      expect(screen.getByRole("link", { name: "Dags" })).not.toHaveAttribute("aria-current");
    });
  });
});
