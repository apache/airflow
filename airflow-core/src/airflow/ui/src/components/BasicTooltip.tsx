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
import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";

type Props = {
  readonly children: ReactNode;
  readonly content: ReactNode;
};

const offset = 8;

export const BasicTooltip = ({ children, content }: Props): ReactElement => {
  const triggerRef = useRef<HTMLSpanElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const [isOpen, setIsOpen] = useState(false);
  const [showOnTop, setShowOnTop] = useState(false);
  const timeoutRef = useRef<NodeJS.Timeout | null>(null);

  const handleMouseEnter = useCallback(() => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }
    timeoutRef.current = setTimeout(() => {
      setIsOpen(true);
    }, 500);
  }, []);

  const handleMouseLeave = useCallback(() => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
      // eslint-disable-next-line unicorn/no-null
      timeoutRef.current = null;
    }
    setIsOpen(false);
  }, []);

  // Calculate position based on actual tooltip height before paint
  useLayoutEffect(() => {
    if (isOpen && triggerRef.current && tooltipRef.current) {
      const triggerRect = triggerRef.current.getBoundingClientRect();
      const tooltipHeight = tooltipRef.current.clientHeight;
      const wouldOverflow = triggerRect.bottom + offset + tooltipHeight > globalThis.innerHeight;

      setShowOnTop(wouldOverflow);
    }
  }, [isOpen]);

  // Cleanup on unmount
  useEffect(
    () => () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    },
    [],
  );

  const trigger = (
    <Box
      as="span"
      display="inline-block"
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      ref={triggerRef}
    >
      {children}
    </Box>
  );

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
          borderRadius="md"
          boxShadow="md"
          color="fg.inverted"
          fontSize="sm"
          left={`${rect.left + scrollX + rect.width / 2}px`}
          paddingX="3"
          paddingY="2"
          pointerEvents="none"
          position="absolute"
          ref={tooltipRef}
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
