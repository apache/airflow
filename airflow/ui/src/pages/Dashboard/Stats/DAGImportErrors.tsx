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
import { Box, Badge, Text, Button, useDisclosure } from "@chakra-ui/react";
import { useState } from "react";
import { FiChevronRight } from "react-icons/fi";

import { DAGImportErrorsModal } from "./DAGImportErrorsModal";

export const DAGImportErrors = () => {
  const { onClose, onOpen, open } = useDisclosure();
  const [importErrorsCount, setImportErrorsCount] = useState<number>(-1);

  const handleImportErrorsCount = (count: number) => {
    setImportErrorsCount(count);
  };

  return (
    <Box alignItems="center" display="flex" gap={2}>
      {importErrorsCount > 0 && (
        <Button
          alignItems="center"
          borderRadius="md"
          display="flex"
          gap={2}
          onClick={onOpen}
          variant="outline"
        >
          <Badge background="red" borderRadius="full" color="white" px={2}>
            {importErrorsCount}
          </Badge>
          <Box alignItems="center" display="flex" gap={1}>
            <Text fontWeight="bold">DAG Import Errors</Text>
            <FiChevronRight />
          </Box>
        </Button>
      )}
      <DAGImportErrorsModal
        onClose={onClose}
        onErrorCountChange={handleImportErrorsCount}
        open={open}
      />
    </Box>
  );
};
