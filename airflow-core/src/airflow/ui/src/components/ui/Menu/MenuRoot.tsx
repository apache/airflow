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
import { Menu as ChakraMenu } from "@chakra-ui/react";
import { createContext, useId } from "react";

export type MenuContextValue = {
  readonly tooltipLabel?: string;
  readonly triggerId?: string;
};

export const MenuContext = createContext<MenuContextValue>({});

type MenuRootProps = {
  readonly tooltipLabel?: string;
} & ChakraMenu.RootProps;

export const Root = ({ children, ids, tooltipLabel, ...rest }: MenuRootProps) => {
  const generatedId = useId();
  const triggerId = Boolean(tooltipLabel) ? generatedId : undefined;

  return (
    <MenuContext.Provider value={{ tooltipLabel, triggerId }}>
      <ChakraMenu.Root ids={{ trigger: triggerId, ...ids }} {...rest}>
        {children}
      </ChakraMenu.Root>
    </MenuContext.Provider>
  );
};
