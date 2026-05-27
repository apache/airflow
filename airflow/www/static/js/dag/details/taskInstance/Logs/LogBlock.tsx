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

import React, {
  useCallback,
  useLayoutEffect,
  useRef,
  useEffect,
  useState,
} from "react";
import { Box, Code } from "@chakra-ui/react";

interface Props {
  parsedLogs: string;
  wrap: boolean;
  tryNumber: number;
  setUnfoldedLogGroup: React.Dispatch<React.SetStateAction<string[]>>;
}

const LogBlock = ({
  parsedLogs,
  wrap,
  tryNumber,
  setUnfoldedLogGroup,
}: Props) => {
  const [autoScroll, setAutoScroll] = useState(true);
  const [logHeight, setLogHeight] = useState<number>();

  const logBoxRef = useRef<HTMLPreElement>(null);

  const resizeLogBlock = useCallback(() => {
    requestAnimationFrame(() => {
      const logBox = logBoxRef.current;
      if (!logBox) return;

      const footerHeight =
        parseInt(getComputedStyle(document.body).paddingBottom, 10) || 0;
      const viewportHeight =
        window.visualViewport?.height ?? window.innerHeight;
      const { top } = logBox.getBoundingClientRect();

      setLogHeight(Math.max(0, viewportHeight - top - footerHeight));
    });
  }, []);

  const scrollToBottom = () => {
    requestAnimationFrame(() => {
      if (logBoxRef.current) {
        logBoxRef.current.scrollTop = logBoxRef.current.scrollHeight;
      }
    });
  };

  useLayoutEffect(() => {
    window.addEventListener("resize", resizeLogBlock);
    window.visualViewport?.addEventListener("resize", resizeLogBlock);

    return () => {
      window.removeEventListener("resize", resizeLogBlock);
      window.visualViewport?.removeEventListener("resize", resizeLogBlock);
    };
  }, [resizeLogBlock]);

  useLayoutEffect(() => {
    resizeLogBlock();
  }, [resizeLogBlock, wrap]);

  useEffect(() => {
    // Always scroll to bottom when wrap or tryNumber change
    if (logBoxRef.current) scrollToBottom();
  }, [wrap, tryNumber]);

  useEffect(() => {
    // Preserve tailing behavior on resize without pulling users away from earlier logs.
    if (autoScroll && logBoxRef.current) scrollToBottom();
  }, [autoScroll, logHeight]);

  useEffect(() => {
    // When logs change, only scroll if autoScroll is enabled
    if (autoScroll && logBoxRef.current) scrollToBottom();
  }, [parsedLogs, autoScroll]);

  const onScroll = (e: React.UIEvent<HTMLPreElement>) => {
    if (e.currentTarget) {
      const { scrollTop, offsetHeight, scrollHeight } = e.currentTarget;
      // Enable autoscroll if we've scrolled to the bottom of the logs
      setAutoScroll(scrollTop + offsetHeight >= scrollHeight);
    }
  };

  const onClick = (e: React.MouseEvent<HTMLElement>) => {
    const target = e.target as HTMLElement;
    const unfoldIdSuffix = "_unfold";
    const foldIdSuffix = "_fold";
    if (target.id?.endsWith(unfoldIdSuffix)) {
      const gId = target.id.substring(
        0,
        target.id.length - unfoldIdSuffix.length,
      );
      // Persist the unfolded state so it survives re-renders (auto-refresh).
      // A new array is required for React to detect the state change.
      setUnfoldedLogGroup((prev) => [...prev, gId]);
      // Immediate DOM update so the user sees the change without waiting
      // for the next render cycle.
      target.style.display = "none";
      if (target.nextElementSibling) {
        (target.nextElementSibling as HTMLElement).style.display = "inline";
      }
    } else if (target.id?.endsWith(foldIdSuffix)) {
      const gId = target.id.substring(
        0,
        target.id.length - foldIdSuffix.length,
      );
      setUnfoldedLogGroup((prev) => prev.filter((id) => id !== gId));
      if (target.parentElement) {
        target.parentElement.style.display = "none";
        if (target.parentElement.previousSibling) {
          (target.parentElement.previousSibling as HTMLElement).style.display =
            "inline";
        }
      }
    }
    return false;
  };

  return (
    <Box flex={1} minH={0} overflow="hidden">
      <Code
        as="pre"
        ref={logBoxRef}
        onScroll={onScroll}
        onClick={onClick}
        height={logHeight === undefined ? "100%" : `${logHeight}px`}
        maxHeight={logHeight === undefined ? "100%" : `${logHeight}px`}
        overflowY="auto"
        overflowX={wrap ? "hidden" : "auto"}
        p={3}
        display="block"
        whiteSpace={wrap ? "pre-wrap" : "pre"}
        border="1px solid"
        borderRadius={3}
        borderColor="blue.500"
      >
        {/* eslint-disable-next-line react/no-danger */}
        <div dangerouslySetInnerHTML={{ __html: parsedLogs }} />
      </Code>
    </Box>
  );
};

export default LogBlock;
