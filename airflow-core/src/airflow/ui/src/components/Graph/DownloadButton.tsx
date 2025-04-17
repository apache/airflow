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
import { IconButton } from "@chakra-ui/react";
import { Panel, useReactFlow, getNodesBounds, getViewportForBounds } from "@xyflow/react";
import { toPng } from "html-to-image";
import { FiDownload } from "react-icons/fi";

import { toaster } from "src/components/ui";

export const DownloadButton = ({ dagId }: { readonly dagId: string }) => {
  const { getNodes, getZoom } = useReactFlow();

  const onClick = () => {
    const nodesBounds = getNodesBounds(getNodes());

    // Method obtained from https://reactflow.dev/examples/misc/download-image
    const container = document.querySelector(".react-flow__viewport");

    if (container instanceof HTMLElement) {
      const dimensions = { height: container.clientHeight, width: container.clientWidth };
      const zoom = getZoom();
      const viewport = getViewportForBounds(nodesBounds, dimensions.width, dimensions.height, zoom, zoom, 2);

      toPng(container, {
        height: dimensions.height,
        style: {
          height: `${dimensions.height}px`,
          transform: `translate(${viewport.x}px, ${viewport.y}px) scale(${viewport.zoom})`,
          width: `${dimensions.width}px`,
        },
        width: dimensions.width,
      })
        .then((dataUrl) => {
          const downloadLink = document.createElement("a");

          downloadLink.setAttribute("download", `${dagId}-graph.png`);
          downloadLink.setAttribute("href", dataUrl);
          downloadLink.click();
        })
        .catch(() => {
          toaster.create({
            description: "Failed to download graph image.",
            title: "Download Failed",
            type: "error",
          });
        });
    }
  };

  return (
    <Panel position="bottom-right" style={{ transform: "translateY(-150px)" }}>
      <IconButton
        aria-label="Download graph image"
        onClick={onClick}
        size="xs"
        title="Download graph image"
        variant="ghost"
      >
        <FiDownload />
      </IconButton>
    </Panel>
  );
};
