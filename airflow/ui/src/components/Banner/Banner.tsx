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
import { Box, HStack, Spacer } from "@chakra-ui/react";

type Props = {
  readonly leftChildren?: Array<React.ReactElement> | undefined;
  readonly rightChildren?: Array<React.ReactElement> | undefined;
  readonly visible: boolean | undefined;
};

const Banner = ({ leftChildren, rightChildren, visible }: Props) =>
  visible ? (
    <Box bg="blue.solid" color="white" fontSize="l" mx="2" my="2" px="5" py="2" rounded="lg">
      <HStack>
        {leftChildren}
        <Spacer flex="max-content" />
        {rightChildren}
      </HStack>
    </Box>
  ) : (
    ""
  );

export default Banner;
