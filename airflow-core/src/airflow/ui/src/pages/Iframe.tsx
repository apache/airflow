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
import { useParams } from "react-router-dom";

import type { ExternalViewResponse } from "openapi/requests/types.gen";

export const Iframe = ({
  externalView,
  sandbox = "allow-forms",
}: {
  readonly externalView: ExternalViewResponse;
  readonly sandbox?: string;
}) => {
  const { dagId, mapIndex, runId, taskId } = useParams();

  // Build the href URL with context parameters if the view has a destination
  let src = externalView.href;

  if (externalView.destination !== undefined && externalView.destination !== "nav") {
    // Check if the href contains placeholders that need to be replaced
    if (dagId !== undefined) {
      src = src.replaceAll("{DAG_ID}", dagId);
    }
    if (runId !== undefined) {
      src = src.replaceAll("{RUN_ID}", runId);
    }
    if (taskId !== undefined) {
      src = src.replaceAll("{TASK_ID}", taskId);
    }
    if (mapIndex !== undefined) {
      src = src.replaceAll("{MAP_INDEX}", mapIndex);
    }
  }

  if (src.startsWith("http://") || src.startsWith("https://")) {
    // URL is absolute
    src = new URL(src).toString();
  }

  return (
    <iframe
      sandbox={sandbox}
      src={src}
      style={{
        border: "none",
        display: "block",
        height: "100%",
        width: "100%",
      }}
      title={externalView.name}
    />
  );
};
