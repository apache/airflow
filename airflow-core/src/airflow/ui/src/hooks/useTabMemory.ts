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
import { useEffect, useRef } from "react";
import { useNavigate } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import type { TabItem } from "./useRequiredActionTabs";

export const TabStorageKeys = {
  DAG: "tab_view_dag",
  MAPPED_TASK_INSTANCE: "tab_view_mapped_ti",
  RUN: "tab_view_run",
  TASK: "tab_view_task",
  TASK_GROUP: "tab_view_task_group",
  TASK_INSTANCE: "tab_view_ti",
} as const;

export type TabStorageKey = (typeof TabStorageKeys)[keyof typeof TabStorageKeys];

type UseTabMemoryOptions = {
  currentPath: string;
  enabled?: boolean;
  storageKey: TabStorageKey;
  tabs: Array<TabItem>;
};

export const useTabMemory = (options: UseTabMemoryOptions) => {
  const { currentPath, enabled = true, storageKey, tabs } = options;
  const navigate = useNavigate();
  const defaultTab = tabs[0]?.value ?? "";
  const [savedTab, setSavedTab] = useLocalStorage<string>(storageKey, defaultTab);

  // Track previous baseUrl to detect entity changes
  const previousBaseUrlRef = useRef<string | null>(null);

  useEffect(() => {
    if (!enabled || tabs.length === 0) {
      return;
    }

    const normalizedPath = currentPath.replace(/\/$/u, "");
    const pathSegments = normalizedPath.split("/");
    const lastSegment = pathSegments[pathSegments.length - 1] ?? "";
    const isTabSegment = tabs.some((tab) => tab.value === lastSegment);

    const baseUrl = isTabSegment && lastSegment !== "" ? pathSegments.slice(0, -1).join("/") : normalizedPath;
    const currentTab = isTabSegment ? lastSegment : "";

    const isAtBase = normalizedPath === baseUrl;
    const isSavedTabValid = Boolean(savedTab) && tabs.some((tab) => tab.value === savedTab);

    // Check if we've switched to a different entity (different baseUrl)
    const hasEntityChanged = previousBaseUrlRef.current !== null && previousBaseUrlRef.current !== baseUrl;
    const isFirstVisit = previousBaseUrlRef.current === null;

    previousBaseUrlRef.current = baseUrl;

    // Check if current tab is valid (including empty string for default tab)
    const isCurrentTabValid = tabs.some((tab) => tab.value === currentTab);

    // Update saved preference when viewing a valid tab
    if (isCurrentTabValid) {
      const isPreviousSavedTabValid = !savedTab || tabs.some((tab) => tab.value === savedTab);

      if (isPreviousSavedTabValid && currentTab !== savedTab) {
        setSavedTab(currentTab);
      }
    }

    const shouldRedirect =
      isAtBase && isSavedTabValid && savedTab !== defaultTab && (isFirstVisit || hasEntityChanged);

    if (shouldRedirect) {
      void Promise.resolve(
        navigate(`${baseUrl}/${savedTab}`, {
          replace: true,
        }),
      );
    }
  }, [currentPath, defaultTab, enabled, navigate, savedTab, setSavedTab, tabs]);
};
