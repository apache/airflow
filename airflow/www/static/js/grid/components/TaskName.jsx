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

import React from 'react';
import {
  Text,
  Flex,
} from '@chakra-ui/react';
import { FiChevronUp, FiChevronDown } from 'react-icons/fi';

const TaskName = ({
  isGroup = false, isMapped = false, onToggle, isOpen = false, level = 0, label,
}) => (
  <Flex
    as={isGroup ? 'button' : 'div'}
    onClick={onToggle}
    aria-label={label}
    title={label}
    mr={4}
    width="100%"
    alignItems="center"
    fontWeight={isGroup || isMapped ? 'bold' : 'normal'}
  >
    <Text
      display="inline"
      ml={level * 4 + 4}
      noOfLines={1}
    >
      {label}
      {isMapped && (
        ' [ ]'
      )}
    </Text>
    {isGroup && (
      isOpen ? <FiChevronDown data-testid="open-group" /> : <FiChevronUp data-testid="closed-group" />
    )}
  </Flex>
);

// Only rerender the component if props change
const MemoizedTaskName = React.memo(TaskName);

export default MemoizedTaskName;
