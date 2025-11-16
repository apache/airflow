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
import { Portal } from "@chakra-ui/react";
import type { CSSProperties, ReactElement, ReactNode } from "react";
import { cloneElement, useCallback, useEffect, useMemo, useRef, useState } from "react";

export type TooltipPlacement =
  | "bottom-end"
  | "bottom-start"
  | "bottom"
  | "left"
  | "right"
  | "top-end"
  | "top-start"
  | "top";

type Props = {
  readonly children: ReactElement;
  readonly content: ReactNode;
  readonly placement?: TooltipPlacement;
};

const calculatePosition = (
  rect: DOMRect,
  placement: TooltipPlacement,
  offset: number,
): { left: string; top: string; transform: string } => {
  const { bottom, height, left, right, top, width } = rect;
  const { scrollX, scrollY } = globalThis;

  switch (placement) {
    case "bottom":
      return {
        left: `${left + scrollX + width / 2}px`,
        top: `${bottom + scrollY + offset}px`,
        transform: "translateX(-50%)",
      };

    case "bottom-end":
      return {
        left: `${right + scrollX}px`,
        top: `${bottom + scrollY + offset}px`,
        transform: "translateX(-100%)",
      };

    case "bottom-start":
      return {
        left: `${left + scrollX}px`,
        top: `${bottom + scrollY + offset}px`,
        transform: "none",
      };

    case "left":
      return {
        left: `${left + scrollX - offset}px`,
        top: `${top + scrollY + height / 2}px`,
        transform: "translate(-100%, -50%)",
      };

    case "right":
      return {
        left: `${right + scrollX + offset}px`,
        top: `${top + scrollY + height / 2}px`,
        transform: "translateY(-50%)",
      };

    case "top":
      return {
        left: `${left + scrollX + width / 2}px`,
        top: `${top + scrollY - offset}px`,
        transform: "translate(-50%, -100%)",
      };

    case "top-end":
      return {
        left: `${right + scrollX}px`,
        top: `${top + scrollY - offset}px`,
        transform: "translate(-100%, -100%)",
      };

    case "top-start":
      return {
        left: `${left + scrollX}px`,
        top: `${top + scrollY - offset}px`,
        transform: "translateY(-100%)",
      };

    default:
      return {
        left: `${left + scrollX + width / 2}px`,
        top: `${top + scrollY - offset}px`,
        transform: "translate(-50%, -100%)",
      };
  }
};

const getArrowStyle = (placement: TooltipPlacement): CSSProperties => {
  const baseStyle: CSSProperties = {
    content: '""',
    height: 0,
    position: "absolute",
    width: 0,
  };

  switch (placement) {
    case "bottom":
    case "bottom-end":
    case "bottom-start":
      return {
        ...baseStyle,
        borderBottom: "4px solid var(--chakra-colors-bg-inverted)",
        borderLeft: "4px solid transparent",
        borderRight: "4px solid transparent",
        left: placement === "bottom" ? "50%" : placement === "bottom-start" ? "12px" : undefined,
        right: placement === "bottom-end" ? "12px" : undefined,
        top: "-4px",
        transform: placement === "bottom" ? "translateX(-50%)" : undefined,
      };

    case "left":
      return {
        ...baseStyle,
        borderBottom: "4px solid transparent",
        borderLeft: "4px solid var(--chakra-colors-bg-inverted)",
        borderTop: "4px solid transparent",
        right: "-4px",
        top: "50%",
        transform: "translateY(-50%)",
      };

    case "right":
      return {
        ...baseStyle,
        borderBottom: "4px solid transparent",
        borderRight: "4px solid var(--chakra-colors-bg-inverted)",
        borderTop: "4px solid transparent",
        left: "-4px",
        top: "50%",
        transform: "translateY(-50%)",
      };

    case "top":
    case "top-end":
    case "top-start":
      return {
        ...baseStyle,
        borderLeft: "4px solid transparent",
        borderRight: "4px solid transparent",
        borderTop: "4px solid var(--chakra-colors-bg-inverted)",
        bottom: "-4px",
        left: placement === "top" ? "50%" : placement === "top-start" ? "12px" : undefined,
        right: placement === "top-end" ? "12px" : undefined,
        transform: placement === "top" ? "translateX(-50%)" : undefined,
      };

    default:
      return baseStyle;
  }
};

export const BasicTooltip = ({ children, content, placement = "bottom" }: Props): ReactElement => {
  const triggerRef = useRef<HTMLElement>(null);
  const [isOpen, setIsOpen] = useState(false);
  const timeoutRef = useRef<NodeJS.Timeout>();

  const offset = 8;
  const zIndex = 1500;

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

  const tooltipStyle = useMemo(() => {
    if (!isOpen || !triggerRef.current) {
      return { display: "none" };
    }

    const rect = triggerRef.current.getBoundingClientRect();
    const position = calculatePosition(rect, placement, offset);

    return {
      ...position,
      backgroundColor: "var(--chakra-colors-bg-inverted)",
      borderRadius: "4px",
      boxShadow: "0 2px 8px rgba(0, 0, 0, 0.15)",
      color: "var(--chakra-colors-fg-inverted)",
      fontSize: "14px",
      padding: "8px 12px",
      pointerEvents: "none" as const,
      position: "absolute" as const,
      whiteSpace: "nowrap" as const,
      zIndex,
    };
  }, [isOpen, placement, offset, zIndex]);

  const arrowStyle = useMemo(() => getArrowStyle(placement), [placement]);

  // Clone children and attach event handlers + ref
  const trigger = cloneElement(children, {
    onMouseEnter: handleMouseEnter,
    onMouseLeave: handleMouseLeave,
    ref: triggerRef,
  });

  return (
    <>
      {trigger}
      {Boolean(isOpen) && (
        <Portal>
          <div style={tooltipStyle}>
            <div style={arrowStyle} />
            {content ?? undefined}
          </div>
        </Portal>
      )}
    </>
  );
};
