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
import { useRef } from "react";

export const useDelayedTooltip = (delayMs: number = 200) => {
  const debounceTimeoutRef = useRef<NodeJS.Timeout | undefined>(undefined);
  const activeTooltipRef = useRef<HTMLElement | undefined>(undefined);

  const handleMouseEnter = (event: React.MouseEvent<HTMLDivElement>) => {
    if (debounceTimeoutRef.current) {
      clearTimeout(debounceTimeoutRef.current);
    }

    const tooltipElement = event.currentTarget.querySelector("[data-tooltip]");

    if (tooltipElement) {
      activeTooltipRef.current = tooltipElement as HTMLElement;
      debounceTimeoutRef.current = setTimeout(() => {
        if (activeTooltipRef.current) {
          activeTooltipRef.current.style.opacity = "1";
          activeTooltipRef.current.style.visibility = "visible";
        }
      }, delayMs);
    }
  };

  const handleMouseLeave = () => {
    if (debounceTimeoutRef.current) {
      clearTimeout(debounceTimeoutRef.current);
      debounceTimeoutRef.current = undefined;
    }

    if (activeTooltipRef.current) {
      activeTooltipRef.current.style.opacity = "0";
      activeTooltipRef.current.style.visibility = "hidden";
      activeTooltipRef.current = undefined;
    }
  };

  return {
    handleMouseEnter,
    handleMouseLeave,
  };
};
