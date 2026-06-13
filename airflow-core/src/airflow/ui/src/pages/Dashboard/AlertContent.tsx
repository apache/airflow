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
import { Box, Button } from "@chakra-ui/react";
import { useLayoutEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import type { UIAlert } from "openapi/requests/types.gen";
import ReactMarkdown from "src/components/ReactMarkdown";
import { Alert } from "src/components/ui";

// Collapse when the rendered content is taller than this many lines. The line
// height is read from the element at runtime, so the clamp matches the real
// rendered text and adapts to the theme and to lines that wrap on narrow screens.
const MAX_VISIBLE_LINES = 5;

export const AlertContent = ({ alert }: { readonly alert: UIAlert }) => {
  const { t: translate } = useTranslation("dashboard");
  const [isExpanded, setIsExpanded] = useState(false);
  const [isOverflowing, setIsOverflowing] = useState(false);
  const [maxHeight, setMaxHeight] = useState<string>();
  const contentRef = useRef<HTMLDivElement>(null);

  useLayoutEffect(() => {
    const element = contentRef.current;

    if (element === null) {
      return undefined;
    }

    const measureAlertHeight = () => {
      const styles = globalThis.getComputedStyle(element);
      const lineHeight = parseFloat(styles.lineHeight) || parseFloat(styles.fontSize) * 1.5;
      const limit = lineHeight * MAX_VISIBLE_LINES;

      setMaxHeight(`${limit}px`);

      // scrollHeight is the full content height regardless of the clamp, so it can
      // be compared to the limit directly. Skip while expanded to keep the toggle.
      if (!isExpanded) {
        setIsOverflowing(element.scrollHeight > limit + 1);
      }
    };

    const observer = new ResizeObserver(measureAlertHeight);

    observer.observe(element);
    measureAlertHeight();

    return () => observer.disconnect();
  }, [isExpanded]);

  return (
    <Alert status={alert.category}>
      <Box maxH={isExpanded ? undefined : maxHeight} overflow="hidden" ref={contentRef} width="100%">
        <ReactMarkdown>{alert.text}</ReactMarkdown>
      </Box>
      {isOverflowing ? (
        <Button
          _hover={{ textDecoration: "underline" }}
          alignSelf="flex-start"
          color="fg.muted"
          mt={1}
          onClick={(event) => {
            // Stop the click from toggling the surrounding accordion trigger.
            event.stopPropagation();
            setIsExpanded((prev) => !prev);
          }}
          px={0}
          size="sm"
          variant="plain"
        >
          {isExpanded ? translate("alerts.seeLessContext") : translate("alerts.seeMoreContext")}
        </Button>
      ) : undefined}
    </Alert>
  );
};
