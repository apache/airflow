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

// Vertical space available to a scroll container: from where the element actually sits in the page
// down to the body's bottom padding (the Flask footer reserve). Anchoring to the viewport this way
// avoids the `height: 100%` cascade, which over-claims because <Tabs>/<TabPanel> have sibling chrome
// (tab headers) on top — that cascade is what truncated the details tabs.
const useContentHeight = (contentRef: React.RefObject<HTMLElement>) => {
  const [height, setHeight] = useState(0);

  useLayoutEffect(() => {
    const update = () => {
      const element = contentRef.current;
      if (!element) return;
      const footerReserve =
        parseInt(
          getComputedStyle(document.body).paddingBottom.replace("px", ""),
          10,
        ) || 0;
      const available = Math.max(
        window.innerHeight -
          element.getBoundingClientRect().top -
          footerReserve,
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
