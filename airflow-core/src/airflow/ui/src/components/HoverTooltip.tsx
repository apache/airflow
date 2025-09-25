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
import { useState, useRef, useCallback, cloneElement } from "react";
import type { ReactElement, ReactNode, RefObject } from "react";

type Props = {
  readonly children: ReactElement;
  readonly delayMs?: number;
  readonly tooltip: (triggerRef: RefObject<HTMLElement>) => ReactNode;
};

export const HoverTooltip = ({ children, delayMs = 200, tooltip }: Props) => {
  const triggerRef = useRef<HTMLElement>(null);
  const [isOpen, setIsOpen] = useState(false);
  const timeoutRef = useRef<NodeJS.Timeout>();

  const handleMouseEnter = useCallback(() => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }
    timeoutRef.current = setTimeout(() => {
      setIsOpen(true);
    }, delayMs);
  }, [delayMs]);

  const handleMouseLeave = useCallback(() => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
      timeoutRef.current = undefined;
    }
    setIsOpen(false);
  }, []);

  const trigger = cloneElement(children, {
    onMouseEnter: handleMouseEnter,
    onMouseLeave: handleMouseLeave,
    ref: triggerRef,
  });

  return (
    <>
      {trigger}
      {Boolean(isOpen) && <Portal>{tooltip(triggerRef)}</Portal>}
    </>
  );
};
