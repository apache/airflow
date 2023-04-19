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

import { debounce } from "lodash";
import React, { useEffect, useState } from "react";

const footer = document.getElementsByTagName("footer")[0];

// For an html element, keep it within view height by calculating the top offset and footer height
const useOffsetTop = (
  contentRef: React.RefObject<HTMLElement>,
  offsetType: "child" | "parent" | "calculate" = "calculate",
  withFooter: boolean = false
) => {
  const [top, setTop] = useState(0);

  useEffect(() => {
    const calculateHeight = debounce(() => {
      const offset = contentRef.current?.getBoundingClientRect().top || 0;

      // Note: offsetParent() will get the highest level parent with position: static;
      const parentOffset =
        contentRef.current?.offsetParent?.getBoundingClientRect().top || 0;

      let fullOffset = offset;
      if (offsetType === "parent") fullOffset = parentOffset;
      else if (offsetType === "calculate") fullOffset = offset - parentOffset;
      if (withFooter) {
        setTop(footer.getBoundingClientRect().top - fullOffset);
      } else {
        setTop(fullOffset);
      }
    }, 25);

    calculateHeight();
    window.addEventListener("resize", calculateHeight);
    return () => {
      window.removeEventListener("resize", calculateHeight);
    };
  }, [contentRef, offsetType, withFooter]);

  return top;
};

export default useOffsetTop;
