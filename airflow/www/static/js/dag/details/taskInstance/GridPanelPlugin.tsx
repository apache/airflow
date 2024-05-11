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

import React from "react";
import { Flex, Box, Text, Divider, Image } from "@chakra-ui/react";

interface Props {
  dagId: string;
  runId: string;
  taskId: string;
  mapIndex?: number | undefined;
  plugins: string[];
}

const GridPanelPlugin = ({
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  dagId,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  runId,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  taskId,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  mapIndex,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  plugins,
}: Props) => (
  // This is a mock - we must iterate over the loaded plugins and load the panel from there
  <Box my={3}>
    <Text as="strong">Grid Panel Example Panel</Text>
    <Flex flexWrap="wrap" mt={3}>
      <Image src="/grid_panel_example/Donkey.jpg" />
    </Flex>
    <Divider my={2} />
  </Box>
);

export default GridPanelPlugin;
