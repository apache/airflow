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

/* Simplified version of chakra's Tooltip component for faster rendering but less customization */

import React from 'react';
import {
  chakra,
  forwardRef,
  HTMLChakraProps,
  omitThemingProps,
  ThemingProps,
  useTheme,
  useTooltip,
  UseTooltipProps,
  Portal,
  PortalProps,
} from '@chakra-ui/react';
import { motion, AnimatePresence } from 'framer-motion';

export interface TooltipProps
  extends HTMLChakraProps<'div'>,
  ThemingProps<'Tooltip'>,
  UseTooltipProps {
  /**
   * The React component to use as the
   * trigger for the tooltip
   */
  children: React.ReactElement
  /**
   * The label of the tooltip
   */
  label?: React.ReactNode
  /**
   * The accessible, human friendly label to use for
   * screen readers.
   *
   * If passed, tooltip will show the content `label`
   * but expose only `aria-label` to assistive technologies
   */
  'aria-label'?: string
  /**
   * If `true`, the tooltip will wrap its children
   * in a `<span/>` with `tabIndex=0`
   */
  shouldWrapChildren?: boolean
  /**
   * If `true`, the tooltip will show an arrow tip
   */
  hasArrow?: boolean
  /**
   * Props to be forwarded to the portal component
   */
  portalProps?: Pick<PortalProps, 'appendToParentPortal' | 'containerRef'>
}

const scale = {
  exit: {
    scale: 0.85,
    opacity: 0,
    transition: {
      opacity: { duration: 0.15, easings: 'easeInOut' },
      scale: { duration: 0.2, easings: 'easeInOut' },
    },
  },
  enter: {
    scale: 1,
    opacity: 1,
    transition: {
      opacity: { easings: 'easeOut', duration: 0.2 },
      scale: { duration: 0.2, ease: [0.175, 0.885, 0.4, 1.1] },
    },
  },
};

const StyledTooltip = chakra(motion.div);

const styles = {
  '--popper-arrow-bg': ['var(--tooltip-bg)'],
  '--tooltip-bg': 'colors.gray.700',
  bg: ['var(--tooltip-bg)'],
  borderRadius: 'sm',
  boxShadow: 'md',
  color: 'whiteAlpha.900',
  fontSize: 'md',
  fontWeight: 'medium',
  maxW: '320px',
  px: '8px',
  py: '2px',
  zIndex: 'tooltip',
};

/**
 * Tooltips display informative text when users hover, focus on, or tap an element.
 *
 * @see Docs     https://chakra-ui.com/docs/overlay/tooltip
 * @see WAI-ARIA https://www.w3.org/TR/wai-aria-practices/#tooltip
 */
const Tooltip = forwardRef<TooltipProps, 'div'>((props, ref) => {
  const ownProps = omitThemingProps(props);
  const theme = useTheme();

  const {
    children,
    label,
    shouldWrapChildren,
    'aria-label': ariaLabel,
    hasArrow,
    bg,
    portalProps,
    background,
    backgroundColor,
    bgColor,
    ...rest
  } = ownProps;

  const tooltip = useTooltip({ ...rest, direction: theme.direction });

  /*
     * Ensure tooltip has only one child node
     */
  const child = React.Children.only(children) as React.ReactElement & {
    ref?: React.Ref<any>
  };
  const trigger: React.ReactElement = React.cloneElement(
    child,
    tooltip.getTriggerProps(child.props, child.ref),
  );

  const tooltipProps = tooltip.getTooltipProps({}, ref);

  /**
   * If the `label` is empty, there's no point showing the tooltip.
   * Let's simply return the children
   */
  if (!label) {
    return children;
  }

  return (
    <>
      {trigger}
      <AnimatePresence>
        {tooltip.isOpen && (
        <Portal {...portalProps}>
          <chakra.div
            {...tooltip.getTooltipPositionerProps()}
            __css={{
              zIndex: styles.zIndex,
              pointerEvents: 'none',
            }}
          >
            <StyledTooltip
              variants={scale}
              {...(tooltipProps as any)}
              initial="exit"
              animate="enter"
              exit="exit"
              __css={styles}
            >
              {label}
              {hasArrow && (
              <chakra.div
                data-popper-arrow
                className="chakra-tooltip__arrow-wrapper"
              >
                <chakra.div
                  data-popper-arrow-inner
                  className="chakra-tooltip__arrow"
                  __css={{ bg: styles.bg }}
                />
              </chakra.div>
              )}
            </StyledTooltip>
          </chakra.div>
        </Portal>
        )}
      </AnimatePresence>
    </>
  );
});

export default Tooltip;
