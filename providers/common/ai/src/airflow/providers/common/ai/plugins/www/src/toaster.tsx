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

import {
  Box,
  createToaster,
  Portal,
  Stack,
  Toaster as ChakraToaster,
  Toast,
} from "@chakra-ui/react";

export const toaster = createToaster({
  pauseOnPageIdle: true,
  placement: "top-end",
});

export const Toaster = () => (
  <Portal>
    <Box
      position="fixed"
      top={4}
      right={4}
      left="auto"
      bottom="auto"
      zIndex={9999}
      maxWidth="min(360px, 100vw)"
      display="flex"
      flexDirection="column"
      alignItems="flex-end"
      gap={2}
    >
      <ChakraToaster toaster={toaster} position="relative" width="auto">
      {(t) => (
        <Toast.Root maxWidth="360px" width="auto">
          <Toast.Indicator />
          <Stack flex="1" gap="1" maxWidth="100%" minWidth={0}>
            {t.title ? <Toast.Title>{t.title}</Toast.Title> : null}
            {t.description ? (
              <Toast.Description overflowWrap="break-word" wordBreak="break-word">
                {t.description}
              </Toast.Description>
            ) : null}
          </Stack>
        </Toast.Root>
      )}
    </ChakraToaster>
    </Box>
  </Portal>
);
