import React from 'react';
import {
  Button,
  IconButton,
  Tooltip,
  useClipboard,
  forwardRef,
} from '@chakra-ui/react';
import { FiCopy } from 'react-icons/fi';

import { useContainerRef } from './context/containerRef';

export const ClipboardButton = forwardRef(
  (
    {
      value,
      variant = 'outline',
      iconOnly = false,
      label = 'copy',
      title = 'Copy',
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
      ...rest,
    };

    return (
      <Tooltip
        label="Copied"
        isOpen={hasCopied}
        isDisabled={!hasCopied}
        closeDelay={500}
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

export const ClipboardText = ({ value }) => (
  <>
    {value}
    <ClipboardButton value={value} iconOnly variant="ghost" size="xs" fontSize="lg" ml={1} />
  </>
);
