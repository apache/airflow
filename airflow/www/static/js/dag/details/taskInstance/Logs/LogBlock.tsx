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
}

const LogBlock = ({ parsedLogs, wrap, tryNumber }: Props) => {
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
    scrollToBottom();
  }, [wrap, tryNumber]);

  useEffect(() => {
    // When logs change, only scroll if autoScroll is enabled
    if (autoScroll) scrollToBottom();
  }, [parsedLogs, autoScroll]);

  const onScroll = (e: React.UIEvent<HTMLDivElement>) => {
    if (e.currentTarget) {
      const { scrollTop, offsetHeight, scrollHeight } = e.currentTarget;
      // Enable autoscroll if we've scrolled to the bottom of the logs
      setAutoScroll(scrollTop + offsetHeight >= scrollHeight);
    }
  };

  return (
    <Code
      ref={logBoxRef}
      onScroll={onScroll}
      maxHeight={`calc(100% - ${offsetTop}px)`}
      overflowY="auto"
      p={3}
      pb={0}
      display="block"
      whiteSpace={wrap ? "pre-wrap" : "pre"}
      border="1px solid"
      borderRadius={3}
      borderColor="blue.500"
    >
      {parsedLogs}
      <div ref={codeBlockBottomDiv} />
    </Code>
  );
};

export default LogBlock;
