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
import { useRef } from "react";

import { useParams } from "react-router-dom";

import { useAssetServiceGetAsset } from "openapi/queries";
import type { ExternalViewResponse } from "openapi/requests/types.gen";

import { useIframeUrlSync } from "src/hooks/useIframeUrlSync";

export const Iframe = ({
  externalView,
  sandbox = "allow-forms",
}: {
  readonly externalView: ExternalViewResponse;
  readonly sandbox?: string;
}) => {
  const { assetId, dagId, mapIndex, page, runId, taskId } = useParams();
  const iframeRef = useRef<HTMLIFrameElement>(null);

  // The asset URI is not part of the route, so resolve it from the asset record. This is a
  // cache hit because the asset details page has already fetched it.
  const { data: asset } = useAssetServiceGetAsset(
    { assetId: assetId === undefined ? 0 : parseInt(assetId, 10) },
    undefined,
    { enabled: Boolean(assetId) },
  );

  // Only standalone (nav) views are deep-linkable; context-scoped embeds (dashboard/overview) keep
  // the placeholder-substituted src and are not synced.
  const isNavView = externalView.destination === undefined || externalView.destination === "nav";

  // Build the href URL with context parameters if the view has a destination
  let src = externalView.href;

  if (externalView.destination !== undefined && externalView.destination !== "nav") {
    // Check if the href contains placeholders that need to be replaced
    if (dagId !== undefined) {
      src = src.replaceAll("{DAG_ID}", encodeURIComponent(dagId));
    }
    if (runId !== undefined) {
      src = src.replaceAll("{RUN_ID}", encodeURIComponent(runId));
    }
    if (taskId !== undefined) {
      src = src.replaceAll("{TASK_ID}", encodeURIComponent(taskId));
    }
    if (mapIndex !== undefined) {
      src = src.replaceAll("{MAP_INDEX}", mapIndex);
    }
    if (assetId !== undefined) {
      src = src.replaceAll("{ASSET_ID}", encodeURIComponent(assetId));
    }
    if (asset?.uri !== undefined) {
      src = src.replaceAll("{ASSET_URI}", encodeURIComponent(asset.uri));
    }
  }

  if (src.startsWith("http://") || src.startsWith("https://")) {
    // URL is absolute
    src = new URL(src).toString();
  }

  const { initialSrc } = useIframeUrlSync({
    basePath: `/plugin/${page ?? ""}`,
    enabled: isNavView,
    entrySrc: src,
    iframeRef,
  });

  return (
    <iframe
      ref={iframeRef}
      sandbox={sandbox}
      src={isNavView ? initialSrc : src}
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
