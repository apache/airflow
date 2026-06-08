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

const MAX_VISIBLE_LINES = 5;

export const AlertContent = ({ alert }: { readonly alert: UIAlert }) => {
  const { t: translate } = useTranslation("dashboard");
  const [isExpanded, setIsExpanded] = useState(false);
  const [isOverflowing, setIsOverflowing] = useState(false);
  const contentRef = useRef<HTMLDivElement>(null);
  const isExpandedRef = useRef(isExpanded);

  // Sync before effects run so the ResizeObserver sees the current value.
  isExpandedRef.current = isExpanded;

  useLayoutEffect(() => {
    const element = contentRef.current;

    if (element === null) {
      return undefined;
    }

    const checkOverflow = () => {
      if (!isExpandedRef.current) {
        setIsOverflowing(element.scrollHeight > element.clientHeight);
      }
    };

    const observer = new ResizeObserver(checkOverflow);

    observer.observe(element);
    checkOverflow();

    return () => observer.disconnect();
  }, []);

  return (
    <Alert status={alert.category}>
      <Box
        ref={contentRef}
        style={
          isExpanded
            ? undefined
            : {
                display: "-webkit-box",
                overflow: "hidden",
                WebkitBoxOrient: "vertical",
                WebkitLineClamp: MAX_VISIBLE_LINES,
              }
        }
        width="100%"
      >
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
