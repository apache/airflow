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
import { Box } from "@chakra-ui/react";
import { useParams } from "react-router-dom";

import { useDagBundleServiceGetDagBundle } from "openapi/queries";

import { ProgressBar } from "src/system-components";

import { ErrorAlert } from "src/components/ErrorAlert";

import { useDagBundleRefetchInterval } from "src/queries/useDagBundleRefetchInterval";
import { useDocumentTitle } from "src/utils";

import { BundleFiles } from "./BundleFiles";
import { Header } from "./Header";

export const DagBundle = () => {
  // The route segment is required, so react-router cannot match this without a name.
  const { bundleName = "" } = useParams();
  const refetchInterval = useDagBundleRefetchInterval();

  const { data, error, isLoading } = useDagBundleServiceGetDagBundle({ bundleName }, undefined, {
    refetchInterval,
    // "Has my deploy landed yet" is exactly what a returning tab is asking, and the version and
    // last-refreshed fields that answer it live on this query.
    refetchOnWindowFocus: refetchInterval !== false,
  });

  useDocumentTitle(bundleName);

  return (
    <Box p={2}>
      <ErrorAlert error={error} />
      <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
      {/* Nothing below renders without a bundle: a header built from undefined asserts an
          inactive, never-refreshed bundle, and the file list would repeat the same 404. */}
      {data === undefined ? undefined : (
        <>
          <Header bundle={data} />
          <BundleFiles bundleName={bundleName} />
        </>
      )}
    </Box>
  );
};
