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

import { Badge, Spinner } from "@chakra-ui/react";
import React, { PropsWithChildren } from "react";

interface Props extends PropsWithChildren {
  hasData: boolean;
  isError: boolean;
}

const LoadingWrapper = ({ children, hasData, isError }: Props) => {
  if (isError) {
    return (
      <Badge colorScheme="red" fontSize="1rem">
        Failed to fetch data
      </Badge>
    );
  }

  // using hasData and not isFetching to now show a spinner on
  // every refresh (autoRefresh)
  if (!hasData) {
    return <Spinner color="blue.500" speed="1s" mr="4px" size="xl" />;
  }

  // Allows to not wrap children in an extra wrapper when it is already
  // a valid component.
  if (React.isValidElement(children)) {
    return children;
  }

  // children is a str, int, etc.
  return <span>{children}</span>;
};

export default LoadingWrapper;
