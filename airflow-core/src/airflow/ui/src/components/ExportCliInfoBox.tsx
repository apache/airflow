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
import { Box, Code, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { Card } from "src/components/ui/Card";

type ExportCliInfoBoxProps = {
  /** Translation key for the description (e.g. "exportCliInfo.connectionsDescription" or "exportCliInfo.variablesDescription") */
  descriptionKey: string;
  /** The CLI export command (e.g. "airflow connections export -" or "airflow variables export -") */
  cliCommand: string;
};

export const ExportCliInfoBox = ({ cliCommand, descriptionKey }: ExportCliInfoBoxProps) => {
  const { t: translate } = useTranslation("admin");

  return (
    <Card.Root variant="elevated" p={4}>
      <Card.Body>
        <Box>
          <Text fontSize="sm" mb={2}>
            {translate(descriptionKey)}
          </Text>
          <Code fontSize="sm">{cliCommand}</Code>
        </Box>
      </Card.Body>
    </Card.Root>
  );
};
