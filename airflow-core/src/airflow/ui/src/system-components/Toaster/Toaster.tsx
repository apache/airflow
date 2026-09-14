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
import { Toaster as ChakraToaster, Portal, Spinner, Stack, Toast } from "@chakra-ui/react";

import { toaster } from "./createToaster";

export const Toaster = () => (
  <Portal>
    <ChakraToaster insetInline={{ mdDown: "4" }} toaster={toaster}>
      {(toast) => (
        <Toast.Root width={{ md: "sm" }}>
          {toast.type === "loading" ? <Spinner color="brand.solid" size="sm" /> : <Toast.Indicator />}
          <Stack flex="1" gap="1" maxWidth="100%">
            {Boolean(toast.title) ? <Toast.Title>{toast.title}</Toast.Title> : undefined}
            {Boolean(toast.description) ? (
              <Toast.Description overflowWrap="break-word" wordBreak="break-word">
                {toast.description}
              </Toast.Description>
            ) : undefined}
          </Stack>
          {toast.action ? <Toast.ActionTrigger>{toast.action.label}</Toast.ActionTrigger> : undefined}
          {Boolean(toast.meta?.closable) ? <Toast.CloseTrigger /> : undefined}
        </Toast.Root>
      )}
    </ChakraToaster>
  </Portal>
);
