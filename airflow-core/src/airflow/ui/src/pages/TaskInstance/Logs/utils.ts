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
import type { Virtualizer } from "@tanstack/react-virtual";

type VirtualizerInstance = Virtualizer<HTMLDivElement, Element>;

type CalculateOffsetOptions = {
  center?: boolean;
  clientHeight: number;
  targetPosition: number;
  totalSize: number;
};

/**
 * Calculate scroll offset, optionally centering the target in the viewport
 */
export const calculateScrollOffset = ({
  center = false,
  clientHeight,
  targetPosition,
  totalSize,
}: CalculateOffsetOptions): number => {
  const anchor = center ? targetPosition - clientHeight / 2 : targetPosition;

  return Math.max(0, Math.min(totalSize - clientHeight, anchor));
};

type ScrollToOffsetOptions = {
  element: HTMLElement;
  offset: number;
  virtualizer: VirtualizerInstance;
};

/**
 * Scroll to a specific offset using virtualizer with fallback
 */
export const scrollToOffset = ({ element, offset, virtualizer }: ScrollToOffsetOptions): void => {
  if (typeof virtualizer.scrollToOffset === "function") {
    try {
      virtualizer.scrollToOffset(offset);
    } catch {
      // Fallback handled below
    }
  }

  element.scrollTop = offset;

  requestAnimationFrame(() => {
    element.scrollTop = offset;
  });
};

type ScrollToTopOptions = {
  element: HTMLElement;
  virtualizer: VirtualizerInstance;
};

/**
 * Scroll to the top of the list
 */
export const scrollToTop = ({ element, virtualizer }: ScrollToTopOptions): void => {
  if (typeof virtualizer.scrollToOffset === "function") {
    try {
      virtualizer.scrollToOffset(0);
    } catch {
      virtualizer.scrollToIndex(0, { align: "start" });
    }
  } else {
    virtualizer.scrollToIndex(0, { align: "start" });
  }

  element.scrollTop = 0;

  requestAnimationFrame(() => {
    element.scrollTop = 0;
  });
};

type ScrollToBottomOptions = {
  element: HTMLElement;
  itemCount: number;
  virtualizer: VirtualizerInstance;
};

/**
 * Scroll to the bottom of the list
 */
export const scrollToBottom = ({ element, itemCount, virtualizer }: ScrollToBottomOptions): void => {
  const total = virtualizer.getTotalSize();
  const clientH = element.clientHeight || 0;
  const offset = Math.max(0, Math.floor(total - clientH));

  if (typeof virtualizer.scrollToOffset === "function") {
    try {
      virtualizer.scrollToOffset(offset);
    } catch {
      virtualizer.scrollToIndex(itemCount - 1, { align: "end" });
    }
  } else {
    virtualizer.scrollToIndex(itemCount - 1, { align: "end" });
  }

  element.scrollTop = offset;

  requestAnimationFrame(() => {
    element.scrollTop = offset;
    requestAnimationFrame(() => {
      element.scrollTop = offset;
      const lastItem = element.querySelector<HTMLElement>(
        `[data-testid="virtualized-item-${itemCount - 1}"]`,
      );

      if (lastItem) {
        lastItem.scrollIntoView({ behavior: "auto", block: "end" });
      }
    });
  });
};
