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
import { useEffect, useMemo, useState } from "react";

import type { ParsedLogEntry } from "src/queries/useLogs";

type VisibleItem = { entry: ParsedLogEntry; originalIndex: number };

/**
 * Manages log group expand/collapse state and computes the list of
 * visible entries for the virtualizer. Handles nested groups and
 * auto-expanding collapsed groups when search navigates into them.
 */
export const useLogGroups = ({
  currentMatchLineIndex,
  expanded,
  parsedLogs,
  searchMatchIndices,
}: {
  currentMatchLineIndex?: number;
  expanded: boolean;
  parsedLogs: Array<ParsedLogEntry>;
  searchMatchIndices?: Set<number>;
}) => {
  // Build parent map for nested visibility checks. Memoized explicitly rather
  // than relying on the compiler: react-virtual force-rerenders the consumer
  // on every scroll event (`flushSync` in its React adapter), and without this
  // memo the O(n) rebuild below re-ran on every one of those renders — cost
  // scaling with total log size, not the virtualized/visible row count.
  const { allGroupIds, groupParentMap } = useMemo(() => {
    const groupHeaders = parsedLogs.filter(
      (entry): entry is { group: NonNullable<ParsedLogEntry["group"]> } & ParsedLogEntry =>
        entry.group?.type === "header",
    );

    return {
      allGroupIds: new Set(groupHeaders.map((entry) => entry.group.id)),
      groupParentMap: new Map<number, number | undefined>(
        groupHeaders.map((entry) => [entry.group.id, entry.group.parentId]),
      ),
    };
  }, [parsedLogs]);

  const [expandedGroups, setExpandedGroups] = useState<Set<number>>(() =>
    expanded ? new Set(allGroupIds) : new Set<number>(),
  );

  // Sync expandedGroups when expanded prop changes (toggle all)
  useEffect(() => {
    if (expanded) {
      setExpandedGroups(new Set(allGroupIds));
    } else {
      setExpandedGroups(new Set<number>());
    }
    // Only react to the expanded prop, not allGroupIds
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [expanded]);

  const toggleGroup = (groupId: number) => {
    setExpandedGroups((prev) => {
      const next = new Set(prev);

      if (next.has(groupId)) {
        next.delete(groupId);
      } else {
        next.add(groupId);
      }

      return next;
    });
  };

  // Build visible items list with index mapping. Memoized for the same reason
  // as groupParentMap above: this loop runs over every log line, and without
  // memoization it re-ran on every react-virtual-forced re-render (i.e. on
  // every scroll event), with cost scaling with total log size rather than
  // the number of rows actually on screen.
  const { lineNumberToVisibleIndex, originalToVisibleIndex, visibleItems } = useMemo(() => {
    // Check if all ancestors of a group are expanded
    const isGroupAncestryExpanded = (groupId: number): boolean => {
      const parentId = groupParentMap.get(groupId);

      if (parentId === undefined) {
        return true;
      }

      return expandedGroups.has(parentId) && isGroupAncestryExpanded(parentId);
    };

    const isEntryVisible = (entry: ParsedLogEntry): boolean => {
      if (!entry.group) {
        return true;
      }

      if (entry.group.type === "header") {
        return isGroupAncestryExpanded(entry.group.id);
      }

      return expandedGroups.has(entry.group.id) && isGroupAncestryExpanded(entry.group.id);
    };

    const items: Array<VisibleItem> = [];
    const originalToVisible = new Map<number, number>();
    const lineNumberToVisible = new Map<number, number>();

    for (let idx = 0; idx < parsedLogs.length; idx += 1) {
      const entry = parsedLogs[idx];

      if (entry && isEntryVisible(entry)) {
        originalToVisible.set(idx, items.length);
        if (entry.lineNumber !== undefined) {
          lineNumberToVisible.set(entry.lineNumber, items.length);
        }
        items.push({ entry, originalIndex: idx });
      }
    }

    return {
      lineNumberToVisibleIndex: lineNumberToVisible,
      originalToVisibleIndex: originalToVisible,
      visibleItems: items,
    };
  }, [expandedGroups, groupParentMap, parsedLogs]);

  // Map search match indices from original to visible indices
  const visibleSearchMatchIndices = searchMatchIndices
    ? new Set(
        [...searchMatchIndices]
          .map((idx) => originalToVisibleIndex.get(idx))
          .filter((idx): idx is number => idx !== undefined),
      )
    : undefined;

  const visibleCurrentMatchIndex =
    currentMatchLineIndex === undefined ? undefined : originalToVisibleIndex.get(currentMatchLineIndex);

  // Auto-expand group (and all ancestors) when search navigates to a match inside a collapsed group
  useEffect(() => {
    if (currentMatchLineIndex === undefined) {
      return;
    }
    const entry = parsedLogs[currentMatchLineIndex];

    if (!entry?.group) {
      return;
    }

    const groupsToExpand: Array<number> = [];

    if (entry.group.type === "line" && !expandedGroups.has(entry.group.id)) {
      groupsToExpand.push(entry.group.id);
    }

    // Walk up the parent chain
    let currentId: number | undefined = entry.group.id;

    while (currentId !== undefined) {
      const parentId = groupParentMap.get(currentId);

      if (parentId !== undefined && !expandedGroups.has(parentId)) {
        groupsToExpand.push(parentId);
      }
      currentId = parentId;
    }

    if (groupsToExpand.length > 0) {
      setExpandedGroups((prev) => new Set([...prev, ...groupsToExpand]));
    }
  }, [currentMatchLineIndex, parsedLogs, expandedGroups, groupParentMap]);

  return {
    expandedGroups,
    lineNumberToVisibleIndex,
    toggleGroup,
    visibleCurrentMatchIndex,
    visibleItems,
    visibleSearchMatchIndices,
  };
};
