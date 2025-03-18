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
import { Button } from "@chakra-ui/react";
import { Panel, useReactFlow, getNodesBounds, getViewportForBounds } from "@xyflow/react";
import { toPng } from "html-to-image";
import { useEffect, useState } from "react";
import { FiDownload } from "react-icons/fi";

export const DownloadButton = () => {
  const { getNodes } = useReactFlow();
  const [dimensions, setDimensions] = useState({ height: 768, width: 1024 });

  useEffect(() => {
    const updateDimensions = () => {
      const container = document.querySelector(".react-flow__viewport");

      if (container) {
        setDimensions({
          height: container.clientHeight,
          width: container.clientWidth,
        });
      }
    };

    updateDimensions();
    globalThis.addEventListener("resize", updateDimensions);

    return () => globalThis.removeEventListener("resize", updateDimensions);
  }, []);

  const onClick = () => {
    const nodesBounds = getNodesBounds(getNodes());
    const viewport = getViewportForBounds(nodesBounds, dimensions.width, dimensions.height, 0.5, 2, 0.25);
    // Method obtained from https://reactflow.dev/examples/misc/download-image
    const container = document.querySelector(".react-flow__viewport");

    if (container instanceof HTMLElement) {
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

          downloadLink.setAttribute("download", "reactflow.png");
          downloadLink.setAttribute("href", dataUrl);
          downloadLink.click();
        })
        .catch(() => {
          // void catch
        });
    }
  };

  return (
    <Panel position="bottom-right">
      <Button colorPalette="blue" onClick={onClick} size="xs">
        <FiDownload />
      </Button>
    </Panel>
  );
};
