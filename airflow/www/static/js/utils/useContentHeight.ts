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

import React, { useLayoutEffect, useState } from "react";

// Vertical space between the top of an element and the bottom of the viewport, less the body's
// bottom padding (the Flask footer reserve). The element's position is taken in document
// coordinates so the result does not change while the page is scrolled — anchoring to the viewport
// instead would feed the element's own height back into the measurement and grow it on every
// observer tick.
const useContentHeight = (contentRef: React.RefObject<HTMLElement>) => {
  const [height, setHeight] = useState(0);

  useLayoutEffect(() => {
    const update = () => {
      const element = contentRef.current;
      if (!element) return;
      const documentTop = element.getBoundingClientRect().top + window.scrollY;
      const footerReserve =
        parseFloat(getComputedStyle(document.body).paddingBottom) || 0;
      const available = Math.max(
        window.innerHeight - footerReserve - documentTop,
        0,
      );
      // Guard against re-setting the same value (avoids ResizeObserver feedback loops).
      setHeight((previous) => (previous === available ? previous : available));
    };

    update();
    window.addEventListener("resize", update);
    const observer = new ResizeObserver(update);
    observer.observe(document.body);
    return () => {
      window.removeEventListener("resize", update);
      observer.disconnect();
    };
  }, [contentRef]);

  return height;
};

export default useContentHeight;
