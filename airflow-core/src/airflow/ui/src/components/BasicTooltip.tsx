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
import { Box, Portal } from "@chakra-ui/react";
import type { ReactElement, ReactNode } from "react";
import { cloneElement, useCallback, useEffect, useRef, useState } from "react";

type Props = {
  readonly children: ReactElement;
  readonly content: ReactNode;
};

const offset = 8;
// Estimated tooltip height for viewport boundary detection
const estimatedTooltipHeight = 100;

export const BasicTooltip = ({ children, content }: Props): ReactElement => {
  const triggerRef = useRef<HTMLElement>(null);
  const [isOpen, setIsOpen] = useState(false);
  const [showOnTop, setShowOnTop] = useState(false);
  const timeoutRef = useRef<NodeJS.Timeout>();

  const handleMouseEnter = useCallback(() => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }
    timeoutRef.current = setTimeout(() => {
      // Check if tooltip would overflow viewport bottom
      if (triggerRef.current) {
        const triggerRect = triggerRef.current.getBoundingClientRect();
        const wouldOverflow = triggerRect.bottom + offset + estimatedTooltipHeight > globalThis.innerHeight;

        setShowOnTop(wouldOverflow);
      }
      setIsOpen(true);
    }, 500);
  }, []);

  const handleMouseLeave = useCallback(() => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
      timeoutRef.current = undefined;
    }
    setIsOpen(false);
  }, []);

  // Cleanup on unmount
  useEffect(
    () => () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    },
    [],
  );

  // Clone children and attach event handlers + ref
  const trigger = cloneElement(children, {
    onMouseEnter: handleMouseEnter,
    onMouseLeave: handleMouseLeave,
    ref: triggerRef,
  });

  if (!isOpen || !triggerRef.current) {
    return trigger;
  }

  const rect = triggerRef.current.getBoundingClientRect();
  const { scrollX, scrollY } = globalThis;

  return (
    <>
      {trigger}
      <Portal>
        <Box
          bg="bg.inverted"
          borderRadius="4px"
          boxShadow="0 2px 8px rgba(0, 0, 0, 0.15)"
          color="fg.inverted"
          fontSize="14px"
          left={`${rect.left + scrollX + rect.width / 2}px`}
          padding="8px 12px"
          pointerEvents="none"
          position="absolute"
          top={showOnTop ? `${rect.top + scrollY - offset}px` : `${rect.bottom + scrollY + offset}px`}
          transform={showOnTop ? "translate(-50%, -100%)" : "translateX(-50%)"}
          whiteSpace="nowrap"
          zIndex="popover"
        >
          <Box
            borderLeft="4px solid transparent"
            borderRight="4px solid transparent"
            height={0}
            left="50%"
            position="absolute"
            transform="translateX(-50%)"
            width={0}
            {...(showOnTop
              ? { borderTop: "4px solid var(--chakra-colors-bg-inverted)", bottom: "-4px" }
              : { borderBottom: "4px solid var(--chakra-colors-bg-inverted)", top: "-4px" })}
          />
          {content}
        </Box>
      </Portal>
    </>
  );
};
