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
import type { ButtonProps } from "@chakra-ui/react";
import { Button, Pagination as ChakraPagination, usePaginationContext } from "@chakra-ui/react";
import { forwardRef } from "react";

import { paginationContext } from "./context";

type PaginationVariant = "outline" | "solid" | "subtle";

const [, useRootProps] = paginationContext;

export type PaginationRootProps = {
  size?: ButtonProps["size"];
  variant?: PaginationVariant;
} & Omit<ChakraPagination.RootProps, "type">;

export const Item = forwardRef<HTMLButtonElement, ChakraPagination.ItemProps>((props, ref) => {
  const { page } = usePaginationContext();
  const { size, variantMap } = useRootProps();

  const current = page === props.value;
  const variant = current ? variantMap.current : variantMap.default;

  return (
    <ChakraPagination.Item ref={ref} {...props} asChild>
      <Button size={size} variant={variant}>
        {props.value}
      </Button>
    </ChakraPagination.Item>
  );
});
