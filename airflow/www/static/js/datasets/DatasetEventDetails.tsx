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
import { Box } from "@chakra-ui/react";

import { useOffsetTop } from "src/utils";

import Graph from "./Graph";

interface Props {
  uri: string;
  timestamp: string;
}

const DatasetEventDetails = ({ uri, timestamp }: Props) => {
  console.log(timestamp);
  const graphRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const offsetTop = useOffsetTop(contentRef);
  const height = `calc(100vh - ${offsetTop + 80}px)`;
  return (
    <Box ref={contentRef}>
      <Box
        ref={graphRef}
        flex={1}
        height={height}
        borderColor="gray.200"
        borderWidth={1}
        position="relative"
      >
        <Graph selectedNodeId={uri} />
      </Box>
      t
    </Box>
  );
};

export default DatasetEventDetails;
