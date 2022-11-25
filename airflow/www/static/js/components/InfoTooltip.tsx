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

import React, { ReactNode } from 'react';
import {
  Box, Tooltip,
} from '@chakra-ui/react';
import { MdInfo } from 'react-icons/md';
import { useContainerRef } from 'src/context/containerRef';
import type { IconBaseProps } from 'react-icons';

interface InfoTooltipProps extends IconBaseProps {
  label: ReactNode;
}

const InfoTooltip = ({ label, ...rest }: InfoTooltipProps) => {
  const containerRef = useContainerRef();
  return (
    <Tooltip
      label={label}
      hasArrow
      placement="top"
      portalProps={{ containerRef }}
    >
      <Box display="inline" ml={1} color="gray.500">
        <MdInfo {...rest} />
      </Box>
    </Tooltip>
  );
};

export default InfoTooltip;
