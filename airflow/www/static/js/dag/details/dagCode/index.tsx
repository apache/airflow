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

import axios, { AxiosResponse } from "axios";
import React, { useRef, useState } from "react";
import { Box, Heading } from "@chakra-ui/react";
import { getMetaValue, useOffsetTop } from "src/utils";
import Time from "src/components/Time";
import type { API } from "src/types";
import CodeBlock from "./CodeBlock";

const DagCode = () => {
  const dagCodeRef = useRef<HTMLDivElement>(null);
  const offsetTop = useOffsetTop(dagCodeRef);

  const [lastParsedTime, setLastParsedTime] = useState("");
  const [dagCode, setDagCode] = useState("");

  const dagApiUrl = getMetaValue("dag_api");
  if (dagApiUrl) {
    axios.get<AxiosResponse, API.DAG>(dagApiUrl).then((dagResp) => {
      setLastParsedTime(dagResp.lastParsedTime ?? "");
      const fileToken =
        dagResp.fileToken !== undefined ? dagResp.fileToken : "";

      let dagSourceApiUrl = getMetaValue("dag_source_api")!;
      if (dagSourceApiUrl && fileToken !== "") {
        dagSourceApiUrl = dagSourceApiUrl.replace("_FILE_TOKEN_", fileToken);
        axios
          .get<AxiosResponse, string>(dagSourceApiUrl, {
            headers: { Accept: "text/plain" },
          })
          .then((dagSourceResp) => {
            setDagCode(dagSourceResp);
          });
      }
    });
  }

  return (
    <Box ref={dagCodeRef} height={`calc(100% - ${offsetTop}px)`}>
      <Heading as="h4" size="md" paddingBottom="10px">
        Parsed at: <Time dateTime={lastParsedTime} />
      </Heading>
      <CodeBlock code={dagCode} language="python" />
    </Box>
  );
};

export default DagCode;
