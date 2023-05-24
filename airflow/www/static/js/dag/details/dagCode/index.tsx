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
import { Box, Heading, Spinner, Alert, AlertIcon } from "@chakra-ui/react";
import { useOffsetTop } from "src/utils";
import Time from "src/components/Time";

import { useDag, useDagCode } from "src/api";
import CodeBlock from "./CodeBlock";

const DagCode = () => {
  const dagCodeRef = useRef<HTMLDivElement>(null);
  const offsetTop = useOffsetTop(dagCodeRef);
  const { data: dagData, isLoading: isLoadingDag, error: dagError } = useDag();
  const {
    data: codeSource = "",
    isLoading: isLoadingCode,
    error: codeError,
  } = useDagCode();

  const isLoading = isLoadingCode || isLoadingDag;
  const error = codeError || dagError;

  return (
    <Box ref={dagCodeRef} height={`calc(100% - ${offsetTop}px)`}>
      {dagData?.lastParsedTime && (
        <Heading as="h4" size="md" paddingBottom="10px" fontSize="14px">
          Parsed at: <Time dateTime={dagData.lastParsedTime} />
        </Heading>
      )}
      {!!error && (
        <Alert status="error" marginBottom="10px">
          <AlertIcon />
          An error occurred while fetching the dag source code.
        </Alert>
      )}
      {isLoading && (
        <Spinner size="xl" color="blue.500" thickness="4px" speed="0.65s" />
      )}

      {codeSource && <CodeBlock code={codeSource} />}
    </Box>
  );
};

export default DagCode;
