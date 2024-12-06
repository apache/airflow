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
import { Box, Heading } from "@chakra-ui/react";
import { useState } from "react";
import { FiBookOpen } from "react-icons/fi";
import Markdown from "react-markdown";

import { Button, Dialog } from "src/components/ui";

const DagDocumentation = ({ docMd }: { readonly docMd: string }) => {
  const [isDocsOpen, setIsDocsOpen] = useState(false);

  return (
    <Box>
      <Button onClick={() => setIsDocsOpen(true)} variant="outline">
        <FiBookOpen height={5} width={5} />
        Dag Docs
      </Button>
      <Dialog.Root
        onOpenChange={() => setIsDocsOpen(false)}
        open={isDocsOpen}
        size="md"
      >
        <Dialog.Content backdrop>
          <Dialog.Header bg="blue.muted">
            <Heading size="xl">Dag Documentation</Heading>
            <Dialog.CloseTrigger closeButtonProps={{ size: "xl" }} />
          </Dialog.Header>
          <Dialog.Body display="flex">
            <Markdown>{docMd}</Markdown>
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </Box>
  );
};

export default DagDocumentation;
