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
import { HStack, IconButton } from "@chakra-ui/react";
import { FiAlignJustify, FiGrid } from "react-icons/fi";

type Display = "card" | "table";

type Props = {
  readonly display: Display;
  readonly setDisplay: (display: Display) => void;
};

export const ToggleTableDisplay = ({ display, setDisplay }: Props) => (
  <HStack pb={2} spacing={1}>
    <IconButton
      aria-label="Show card view"
      colorScheme="blue"
      height={8}
      icon={<FiGrid />}
      isActive={display === "card"}
      minWidth={8}
      onClick={() => setDisplay("card")}
      variant="outline"
      width={8}
    />
    <IconButton
      aria-label="Show table view"
      colorScheme="blue"
      height={8}
      icon={<FiAlignJustify />}
      isActive={display === "table"}
      minWidth={8}
      onClick={() => setDisplay("table")}
      variant="outline"
      width={8}
    />
  </HStack>
);
