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

/* global document */

import React, { useEffect } from 'react';

const useContentHeight = (contentRef: React.RefObject<HTMLDivElement>) => {
  useEffect(() => {
    const calculateHeight = () => {
      if (contentRef.current) {
        const topOffset = contentRef.current.offsetTop;
        const footerHeight = parseInt(getComputedStyle(document.getElementsByTagName('body')[0]).paddingBottom.replace('px', ''), 10) || 0;
        const newHeight = window.innerHeight - topOffset - footerHeight;
        const newHeightPx = `${newHeight}px`;

        // only set a new height if it has changed
        if (newHeightPx !== contentRef.current.style.height) {
          // keep a minimum usable height of 300px
          contentRef.current.style.height = newHeight > 300 ? newHeightPx : '300px';
        }
      }
    };
    // set height on load
    calculateHeight();

    // set height on window resize
    window.addEventListener('resize', calculateHeight);
    return () => {
      window.removeEventListener('resize', calculateHeight);
    };
  }, [contentRef]);
};

export default useContentHeight;
