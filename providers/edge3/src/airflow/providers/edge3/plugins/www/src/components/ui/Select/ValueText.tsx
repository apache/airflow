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
import type { CollectionItem } from "@chakra-ui/react";
import { Select as ChakraSelect } from "@chakra-ui/react";
import { forwardRef } from "react";

type ValueTextProps = {
  readonly children?: (items: Array<CollectionItem>) => React.ReactNode;
} & Omit<ChakraSelect.ValueTextProps, "children">;

export const ValueText = forwardRef<HTMLSpanElement, ValueTextProps>((props, ref) => {
  const { children, ...rest } = props;

  return (
    <ChakraSelect.ValueText {...rest} ref={ref}>
      <ChakraSelect.Context>
        {(select) => {
          const items = select.selectedItems;

          if (items.length === 0) {
            return props.placeholder;
          }
          if (children) {
            return children(items);
          }
          if (items.length === 1) {
            return select.collection.stringifyItem(items[0]);
          }

          return `${items.length} selected`;
        }}
      </ChakraSelect.Context>
    </ChakraSelect.ValueText>
  );
});
