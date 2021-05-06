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

/* eslint-disable no-underscore-dangle */
import React from 'react';
import Select, { components as selectComponents, Props as SelectProps } from 'react-select';
import {
  Flex,
  Tag,
  TagCloseButton,
  TagLabel,
  Divider,
  CloseButton,
  Center,
  Box,
  Portal,
  StylesProvider,
  useMultiStyleConfig,
  useStyles,
  useTheme,
  useColorModeValue,
  RecursiveCSSObject,
  CSSWithMultiValues,
} from '@chakra-ui/react';
import { FiChevronDown } from 'react-icons/fi';

const MultiSelect = ({
  name = '',
  styles = {},
  components = {},
  ...props
}: SelectProps): JSX.Element => {
  const chakraTheme = useTheme();

  const placeholderColor = useColorModeValue(
    chakraTheme.colors.gray[400],
    chakraTheme.colors.whiteAlpha[400],
  );

  const inputColor = useColorModeValue(
    chakraTheme.colors.gray[800],
    chakraTheme.colors.whiteAlpha[900],
  );

  const chakraStyles = {
    input: (provided: Record<string, any>) => ({
      ...provided,
      color: inputColor,
      lineHeight: 1,
    }),
    singleValue: (provided: Record<string, any>) => ({
      ...provided,
      color: inputColor,
      lineHeight: 1,
    }),
    menu: (provided: Record<string, any>) => ({
      ...provided,
      boxShadow: 'none',
    }),
    valueContainer: (provided: Record<string, any>) => ({
      ...provided,
      padding: '0.125rem 1rem',
    }),
  };

  return (
    <Select
      name={name}
      components={{
        ...{
          Control: ({
            children, innerRef, innerProps, isDisabled, isFocused,
          }) => {
            const inputStyles = useMultiStyleConfig('Input', {});
            return (
              <StylesProvider value={inputStyles}>
                <Flex
                  ref={innerRef}
                  sx={{
                    ...inputStyles.field,
                    p: 0,
                    overflow: 'hidden',
                    h: 'auto',
                    minH: 10,
                  }}
                  {...innerProps}
                  {...(isFocused && { 'data-focus': true })}
                  {...(isDisabled && { disabled: true })}
                >
                  {children}
                </Flex>
              </StylesProvider>
            );
          },
          MultiValueContainer: ({
            children,
            innerRef,
            innerProps,
            data: { isFixed },
          }) => (
            <Tag
              ref={innerRef}
              {...innerProps}
              m="0.125rem"
              variant={isFixed ? 'solid' : 'subtle'}
            >
              {children}
            </Tag>
          ),
          MultiValueLabel: ({ children, innerRef, innerProps }) => (
            <TagLabel ref={innerRef} {...innerProps}>
              {children}
            </TagLabel>
          ),
          MultiValueRemove: ({
            children, innerRef, innerProps, data: { isFixed },
          }) => {
            if (isFixed) {
              return null;
            }

            return (
              <TagCloseButton ref={innerRef} {...innerProps}>
                {children}
              </TagCloseButton>
            );
          },
          IndicatorSeparator: ({ innerProps }) => (
            <Divider
              {...innerProps}
              orientation="vertical"
              opacity="1"
            />
          ),
          ClearIndicator: ({ innerProps }) => (
            <CloseButton {...innerProps} size="sm" mx={2} />
          ),
          DropdownIndicator: ({ innerProps }) => {
            const { addon } = useStyles();

            return (
              <Center
                {...innerProps}
                sx={{
                  ...addon,
                  h: '100%',
                  p: 0,
                  borderRadius: 0,
                  borderWidth: 0,
                  cursor: 'pointer',
                }}
              >
                <FiChevronDown height={5} width={5} />
              </Center>
            );
          },
          // Menu components
          MenuPortal: ({ children, ...portalProps }) => (
            <Portal {...portalProps}>
              {children}
            </Portal>
          ),
          Menu: ({ children, ...menuProps }) => {
            const menuStyles = useMultiStyleConfig('Menu', {});
            return (
              <selectComponents.Menu {...menuProps}>
                <StylesProvider value={menuStyles}>{children}</StylesProvider>
              </selectComponents.Menu>
            );
          },
          MenuList: ({
            innerRef, children, maxHeight,
          }) => {
            const { list } = useStyles();
            return (
              <Box
                sx={{
                  ...list,
                  maxH: `${maxHeight}px`,
                  overflowY: 'auto',
                }}
                ref={innerRef}
              >
                {children}
              </Box>
            );
          },
          GroupHeading: ({ innerProps, children }) => {
            const { groupTitle } = useStyles();
            return (
              <Box sx={groupTitle} {...innerProps}>
                {children}
              </Box>
            );
          },
          Option: ({
            innerRef, innerProps, children, isFocused, isDisabled,
          }) => {
            const { item } = useStyles();
            interface ItemProps extends CSSWithMultiValues {
              _disabled: CSSWithMultiValues,
              _focus: CSSWithMultiValues,
            }
            return (
              <Box
                sx={{
                  ...item,
                  w: '100%',
                  textAlign: 'left',
                  cursor: 'pointer',
                  bg: isFocused ? (item as RecursiveCSSObject<ItemProps>)._focus.bg : 'transparent',
                  ...(isDisabled && (item as RecursiveCSSObject<ItemProps>)._disabled),
                }}
                ref={innerRef}
                {...innerProps}
                {...(isDisabled && { disabled: true })}
              >
                {children}
              </Box>
            );
          },
        },
        ...components,
      }}
      styles={{
        ...chakraStyles,
        ...styles,
      }}
      theme={(baseTheme) => ({
        ...baseTheme,
        borderRadius: chakraTheme.radii.md,
        colors: {
          ...baseTheme.colors,
          neutral50: placeholderColor, // placeholder text color
          neutral40: placeholderColor, // noOptionsMessage color
        },
      })}
      {...props}
    />
  );
};

export default MultiSelect;
