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

/* global document, window */

import { debounce } from 'lodash';
import React, { useEffect, useState } from 'react';

const footerHeight = parseInt(getComputedStyle(document.getElementsByTagName('body')[0]).paddingBottom.replace('px', ''), 10) || 0;

// For an html element, keep it within view height by calculating the top offset and footer height
const useOffsetHeight = (
  contentRef: React.RefObject<HTMLDivElement | HTMLPreElement>,
  dataToWatch?: any, // recalculate height if this changes
  minHeight: number = 300,
) => {
  const [height, setHeight] = useState(0);

  useEffect(() => {
    const calculateHeight = debounce(() => {
      if (contentRef.current) {
        const topOffset = contentRef.current.offsetTop;
        const newHeight = window.innerHeight - (topOffset + footerHeight);
        setHeight(newHeight > minHeight ? newHeight : minHeight);
      }
    }, 25);
    // set height on load
    calculateHeight();

    // set height on window resize
    window.addEventListener('resize', calculateHeight);
    return () => {
      window.removeEventListener('resize', calculateHeight);
    };
  }, [contentRef, minHeight, dataToWatch]);

  return height;
};

export default useOffsetHeight;
