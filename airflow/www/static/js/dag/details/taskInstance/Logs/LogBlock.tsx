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

import React, { useRef, useEffect, useState } from "react";
import { Code } from "@chakra-ui/react";
import { useOffsetTop } from "src/utils";

interface Props {
  parsedLogs: string;
  wrap: boolean;
  tryNumber: number;
  unfoldedGroups: Array<string>;
  setUnfoldedLogGroup: React.Dispatch<React.SetStateAction<string[]>>;
}

const LogBlock = ({
  parsedLogs,
  wrap,
  tryNumber,
  unfoldedGroups,
  setUnfoldedLogGroup,
}: Props) => {
  const [autoScroll, setAutoScroll] = useState(true);

  const logBoxRef = useRef<HTMLPreElement>(null);
  const codeBlockBottomDiv = useRef<HTMLDivElement>(null);
  const offsetTop = useOffsetTop(logBoxRef);

  const scrollToBottom = () => {
    codeBlockBottomDiv.current?.scrollIntoView({
      block: "nearest",
      inline: "nearest",
    });
  };

  useEffect(() => {
    // Always scroll to bottom when wrap or tryNumber change
    if (offsetTop) scrollToBottom();
  }, [wrap, tryNumber, offsetTop]);

  useEffect(() => {
    // When logs change, only scroll if autoScroll is enabled
    if (autoScroll && offsetTop) scrollToBottom();
  }, [parsedLogs, autoScroll, offsetTop]);

  const onScroll = (e: React.UIEvent<HTMLDivElement>) => {
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
        target.id.length - unfoldIdSuffix.length
      );
      // remember the folding state if logs re-loaded
      unfoldedGroups.push(gId);
      setUnfoldedLogGroup(unfoldedGroups);
      // now do the folding
      target.style.display = "none";
      if (target.nextElementSibling) {
        (target.nextElementSibling as HTMLElement).style.display = "inline";
      }
    } else if (target.id?.endsWith(foldIdSuffix)) {
      const gId = target.id.substring(
        0,
        target.id.length - foldIdSuffix.length
      );
      // remember the folding state if logs re-loaded
      if (unfoldedGroups.indexOf(gId) >= 0) {
        unfoldedGroups.splice(unfoldedGroups.indexOf(gId), 1);
      }
      setUnfoldedLogGroup(unfoldedGroups);
      // now do the folding
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
    <Code
      ref={logBoxRef}
      onScroll={onScroll}
      onClick={onClick}
      maxHeight={`calc(100% - ${offsetTop}px)`}
      overflowY="auto"
      p={3}
      display="block"
      whiteSpace={wrap ? "pre-wrap" : "pre"}
      border="1px solid"
      borderRadius={3}
      borderColor="blue.500"
    >
      <div dangerouslySetInnerHTML={{ __html: parsedLogs }} />
      <div ref={codeBlockBottomDiv} />
    </Code>
  );
};

export default LogBlock;
