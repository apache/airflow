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
import { HStack, type StackProps } from "@chakra-ui/react";

/**
 * The bar the crumbs sit in. `overflow="hidden"` is what clips the end segments' hover fill to the
 * rounded corners, and `width="fit-content"` keeps the bar hugging its crumbs rather than the header.
 */
export const BreadcrumbRow = ({ children, ...rest }: StackProps) => (
  <HStack
    alignItems="stretch"
    as="nav"
    borderRadius="md"
    gap={0}
    overflow="hidden"
    width="fit-content"
    {...rest}
  >
    {children}
  </HStack>
);
