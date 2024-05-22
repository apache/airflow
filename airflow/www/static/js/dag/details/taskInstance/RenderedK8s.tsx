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

import React, { useRef } from "react";
import { Code } from "@chakra-ui/react";
import YAML from "json-to-pretty-yaml";

import { getMetaValue, useOffsetTop } from "src/utils";

import useSelection from "src/dag/useSelection";
import { useRenderedK8s } from "src/api";

const isK8sExecutor = getMetaValue("k8s_or_k8scelery_executor") === "True";

const RenderedK8s = () => {
  const {
    selected: { runId, taskId, mapIndex },
  } = useSelection();

  const { data: renderedK8s } = useRenderedK8s(runId, taskId, mapIndex);

  const k8sRef = useRef<HTMLPreElement>(null);
  const offsetTop = useOffsetTop(k8sRef);

  if (!isK8sExecutor || !runId || !taskId) return null;

  return (
    <Code
      mt={3}
      ref={k8sRef}
      maxHeight={`calc(100% - ${offsetTop}px)`}
      overflowY="auto"
    >
      <pre>{YAML.stringify(renderedK8s)}</pre>
    </Code>
  );
};

export default RenderedK8s;
