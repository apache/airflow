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

/* global localStorage */

import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  Flex,
  useDisclosure,
  Divider,
  Spinner,
} from '@chakra-ui/react';
import { isEmpty, debounce } from 'lodash';

import useSelection from 'src/dag/useSelection';
import { useGridData } from 'src/api';
import { hoverDelay } from 'src/utils';

import Details from './details';
import Grid from './grid';
import FilterBar from './nav/FilterBar';
import LegendRow from './nav/LegendRow';

const detailsPanelKey = 'hideDetailsPanel';

const Main = () => {
  const { data: { groups }, isLoading } = useGridData();
  const resizeRef = useRef<HTMLDivElement>(null);
  const gridRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const isPanelOpen = localStorage.getItem(detailsPanelKey) !== 'true';
  const { isOpen, onToggle } = useDisclosure({ defaultIsOpen: isPanelOpen });
  const { clearSelection } = useSelection();
  const [hoveredTaskState, setHoveredTaskState] = useState<string | null | undefined>();

  // Add a debounced delay to not constantly trigger highlighting certain task states
  const onStatusHover = debounce((state) => setHoveredTaskState(state), hoverDelay);

  const onStatusLeave = () => {
    setHoveredTaskState(undefined);
    onStatusHover.cancel();
  };

  const onPanelToggle = () => {
    if (!isOpen) {
      localStorage.setItem(detailsPanelKey, 'false');
    } else {
      clearSelection();
      localStorage.setItem(detailsPanelKey, 'true');
    }
    onToggle();
  };

  useEffect(() => {
    if (contentRef.current) {
      const topOffset = contentRef.current.offsetTop;
      const footerHeight = parseInt(getComputedStyle(document.getElementsByTagName('body')[0]).paddingBottom.replace('px', '').replace('em', ''), 10) || 0;
      contentRef.current.style.height = `${window.innerHeight - topOffset - footerHeight}px`;
    }
  }, []);

  useEffect(() => {
    const resize = (e: any) => {
      if (gridRef.current && e.x > 350 && e.x < window.innerWidth - 400) {
        gridRef.current.style.width = `${e.x}px`;
      }
    };

    const resizeEl = resizeRef.current;
    if (resizeEl) {
      resizeEl?.addEventListener('mousedown', (e) => {
        e.preventDefault();
        document.addEventListener('mousemove', resize, false);
      }, false);

      document.addEventListener('mouseup', () => {
        document.removeEventListener('mousemove', resize, false);
      }, false);
    }
    return () => {
      resizeEl?.removeEventListener('mousedown', resize);
      document.removeEventListener('mouseup', resize);
    };
  }, []);

  return (
    <Box flex={1}>
      <FilterBar />
      <LegendRow onStatusHover={onStatusHover} onStatusLeave={onStatusLeave} />
      <Divider mb={5} borderBottomWidth={2} />
      <Flex ref={contentRef}>
        {isLoading || isEmpty(groups)
          ? (<Spinner />)
          : (
            <>
              <Box
                minWidth="350px"
                flex={isOpen ? undefined : 1}
                ref={gridRef}
                height="100%"
              >
                <Grid
                  isPanelOpen={isOpen}
                  onPanelToggle={onPanelToggle}
                  hoveredTaskState={hoveredTaskState}
                />
              </Box>
              {isOpen && (
                <>
                  <Box
                    width={2}
                    cursor="ew-resize"
                    bg="gray.200"
                    ref={resizeRef}
                    zIndex={1}
                  />
                  <Box
                    flexGrow={1}
                    minWidth="400px"
                    zIndex={1}
                    bg="white"
                    height="100%"
                  >
                    <Details />
                  </Box>
                </>
              )}
            </>
          )}
      </Flex>
    </Box>
  );
};

export default Main;
