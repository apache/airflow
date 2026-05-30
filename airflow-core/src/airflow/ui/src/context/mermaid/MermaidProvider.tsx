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
import type { PropsWithChildren } from "react";

import { useColorMode } from "src/context/colorMode";

import { MermaidContext, type MermaidRenderParams, type MermaidTheme } from "./Context";

type MermaidApi = {
  initialize: (config: { securityLevel: "strict"; startOnLoad: false; theme: MermaidTheme }) => void;
  render: (diagramId: string, chart: string) => Promise<{ svg: string }>;
};

let mermaidModulePromise: Promise<MermaidApi> | undefined;

const getMermaid = async (): Promise<MermaidApi> => {
  mermaidModulePromise ??= import("mermaid").then(({ default: mermaid }) => mermaid as MermaidApi);

  return mermaidModulePromise;
};

export const MermaidProvider = ({ children }: PropsWithChildren) => {
  const { colorMode } = useColorMode();
  const theme: MermaidTheme = colorMode === "dark" ? "dark" : "default";

  const renderDiagram = async ({ chart, diagramId }: MermaidRenderParams): Promise<string> => {
    const mermaid = await getMermaid();

    mermaid.initialize({ securityLevel: "strict", startOnLoad: false, theme });

    const { svg } = await mermaid.render(diagramId, chart);

    return svg;
  };

  const value = { renderDiagram };

  return <MermaidContext.Provider value={value}>{children}</MermaidContext.Provider>;
};
