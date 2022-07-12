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
  Button,
  IconButton,
  Tooltip,
  useClipboard,
  forwardRef,
} from '@chakra-ui/react';
import { FiCopy } from 'react-icons/fi';

import { useContainerRef } from 'grid/context/containerRef';

export const ClipboardButton = forwardRef(
  (
    {
      value,
      variant = 'outline',
      iconOnly = false,
      label = 'copy',
      title = 'Copy',
      colorScheme = 'blue',
      'aria-label': ariaLabel = 'Copy',
      ...rest
    },
    ref,
  ) => {
    const { hasCopied, onCopy } = useClipboard(value);
    const containerRef = useContainerRef();

    const commonProps = {
      onClick: onCopy,
      variant,
      title,
      ref,
      colorScheme,
      ...rest,
    };

    return (
      <Tooltip
        label="Copied"
        isOpen={hasCopied}
        isDisabled={!hasCopied}
        placement="top"
        portalProps={{ containerRef }}
      >
        {iconOnly ? (
          <IconButton icon={<FiCopy />} aria-label={ariaLabel} {...commonProps} />
        ) : (
          <Button leftIcon={<FiCopy />} {...commonProps}>
            {label}
          </Button>
        )}
      </Tooltip>
    );
  },
);

interface Props {
  value: string
}

export const ClipboardText = ({ value }: Props) => (
  <>
    {value}
    <ClipboardButton value={value} iconOnly variant="ghost" size="xs" fontSize="xl" ml={1} />
  </>
);
