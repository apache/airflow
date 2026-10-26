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

import { getPluginIconSrc } from "./pluginIcon";

const icons = { icon: "light.svg", icon_dark_mode: "dark.svg" };

describe("getPluginIconSrc", () => {
  it("uses the plain icon in light mode", () => {
    expect(getPluginIconSrc(icons, "light")).toBe("light.svg");
  });

  it("uses the dark icon in dark mode", () => {
    expect(getPluginIconSrc(icons, "dark")).toBe("dark.svg");
  });

  // `icon_dark_mode` is optional, so one icon has to serve both themes.
  it("falls back to the plain icon in dark mode when no dark one is given", () => {
    expect(getPluginIconSrc({ icon: "light.svg" }, "dark")).toBe("light.svg");
  });

  it("ignores a dark icon explicitly set to null", () => {
    expect(getPluginIconSrc({ icon: "light.svg", icon_dark_mode: null }, "dark")).toBe("light.svg");
  });

  // The caller draws its own default rather than being handed a placeholder URL.
  it.each([
    { contribution: {}, label: "no icons at all" },
    { contribution: { icon: null }, label: "an explicitly null icon" },
  ])("returns undefined for $label", ({ contribution }) => {
    expect(getPluginIconSrc(contribution, "light")).toBeUndefined();
  });

  it("returns undefined rather than a dark icon when there is no plain one", () => {
    // A contribution supplying only a dark icon is malformed; light mode has nothing to show.
    expect(getPluginIconSrc({ icon_dark_mode: "dark.svg" }, "light")).toBeUndefined();
  });

  it("treats an unknown colour mode as light", () => {
    expect(getPluginIconSrc(icons, undefined)).toBe("light.svg");
  });
});
