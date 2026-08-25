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
import type { Mermaid } from "mermaid";

type MermaidTheme = "dark" | "default";

let initializedTheme: MermaidTheme | undefined;
let mermaidModulePromise: Promise<Mermaid> | undefined;

export const renderMermaidDiagram = async ({
  chart,
  diagramId,
  theme,
}: {
  readonly chart: string;
  readonly diagramId: string;
  readonly theme: MermaidTheme;
}): Promise<string> => {
  mermaidModulePromise ??= import("mermaid").then((module) => module.default);

  const mermaid = await mermaidModulePromise;

  if (initializedTheme !== theme) {
    mermaid.initialize({ securityLevel: "strict", startOnLoad: false, theme });
    initializedTheme = theme;
  }

  const { svg } = await mermaid.render(diagramId, chart);

  return svg;
};
