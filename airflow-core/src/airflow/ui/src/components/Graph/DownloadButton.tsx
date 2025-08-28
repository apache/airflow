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

/*
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
import { IconButton } from "@chakra-ui/react";
import { Panel, useReactFlow } from "@xyflow/react";
import { toPng } from "html-to-image";
import { FiDownload } from "react-icons/fi";

import { toaster } from "src/components/ui";

export const DownloadButton = ({ name }: { readonly name: string }) => {
  const { fitView } = useReactFlow();

  const onClick = async () => {
    // Ensure the graph fits before taking screenshot
    fitView({ duration: 0, padding: 0.1 });

    // Small delay to let layout stabilize
    await new Promise((resolve) => setTimeout(resolve, 50));

    const viewport = document.querySelector(".react-flow__viewport") as HTMLElement;

    if (!viewport) {
      toaster.create({ description: "Graph element not found." });

      return;
    }

    try {
      const dataUrl = await toPng(viewport, {
        backgroundColor: "#ffffff",
        pixelRatio: 2,
      });

      const link = document.createElement("a");

      link.download = `${name}-graph.png`;
      link.href = dataUrl;
      document.body.append(link);
      link.click();
      link.remove();
    } catch {
      toaster.create({
        description: "An error occurred while generating the image.",
        title: "Download Failed",
        type: "error",
      });
    }
  };

  return (
    <Panel position="bottom-right" style={{ transform: "translateY(-150px)" }}>
      <IconButton
        aria-label="Download graph image"
        onClick={() => {
          void onClick(); // explicitly mark as fire-and-forget
        }}
        size="xs"
        title="Download graph image"
        variant="ghost"
      >
        <FiDownload />
      </IconButton>
    </Panel>
  );
};
