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

import React, { ReactNode, useState } from 'react';
import { Box, chakra } from '@chakra-ui/react';

interface Props {
  delay?: number;
  direction?: 'top' | 'bottom' | 'left' | 'right';
  content: string | ReactNode;
}

const Tooltip: React.FC<Props> = ({
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  delay = 400, direction = 'top', content, children,
}) => {
  let timeout: any;
  const [active, setActive] = useState(false);

  const showTip = () => {
    timeout = setTimeout(() => {
      setActive(true);
    }, delay);
  };

  const hideTip = () => {
    clearInterval(timeout);
    setActive(false);
  };

  return (
    <chakra.span
      onMouseEnter={showTip}
      onMouseLeave={hideTip}
      position="relative"
    >
      {children}
      {active && (
        <Box
          position="absolute"
          borderRadius={2}
          left="50%"
          transform="translateX(-50%)"
          p={3}
          color="white"
          backgroundColor="black"
          zIndex={100}
          width={200}
        >
          {content}
        </Box>
      )}
    </chakra.span>
  );
};

export default Tooltip;
