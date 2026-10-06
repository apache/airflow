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

import { Badge, Box, Flex, HStack, Heading, SimpleGrid, Spinner, Text } from "@chakra-ui/react";
import type { FC, ReactNode } from "react";
import { LuInfo } from "react-icons/lu";

import { NoModelInfo } from "src/components/NoModelInfo";
import { Tooltip } from "src/components/Tooltip";
import { useModelInfo } from "src/hooks/useModelInfo";

interface ModelPanelProps {
  dagId: string;
  runId: string;
  taskId: string;
  mapIndex: number;
}

// Pydantic-ai prices a response via genai-prices' per-token rate lookup (see
// durable/replay_usage.py::fill_replayed_cost), not from provider-reported billing --
// a model or provider the lookup doesn't recognise is left unpriced rather than guessed.
const COST_TOOLTIP =
  "Estimated in USD from a per-token price lookup (genai-prices), not provider-reported " +
  "billing -- best-effort, and may be stale or unavailable for some models/providers.";

const StatBox: FC<{ label: string; tooltip?: ReactNode; value: number | string }> = ({
  label,
  tooltip,
  value,
}) => (
  <Box bg="bg.subtle" borderRadius="lg" borderWidth="1px" p={4}>
    <HStack gap={0.5}>
      <Text color="fg.muted" fontSize="xs">
        {label}
      </Text>
      {tooltip !== undefined && (
        <Tooltip content={tooltip} portalled>
          <Box aria-label={`About ${label}`} as="button" color="fg.muted" p={0.5}>
            <LuInfo />
          </Box>
        </Tooltip>
      )}
    </HStack>
    <Text fontSize="lg" fontWeight="semibold">
      {value}
    </Text>
  </Box>
);

export const ModelPanel: FC<ModelPanelProps> = ({ dagId, runId, taskId, mapIndex }) => {
  const { usage, loading, error } = useModelInfo(dagId, runId, taskId, mapIndex);
  const modelName = usage?.model_name ?? null;

  if (loading) {
    return (
      <Flex align="center" gap={2} p={2}>
        <Spinner colorPalette="brand" size="sm" />
        <Text color="fg.muted" fontSize="sm">
          Loading model info...
        </Text>
      </Flex>
    );
  }

  if (!usage) {
    return <NoModelInfo error={error} />;
  }

  return (
    <Box p={2}>
      <Box borderBottomWidth="1px" mb={4} pb={4}>
        <Heading size="sm">AI Model</Heading>
        <HStack color="fg.muted" fontSize="sm" gap={3} mt={1}>
          <Text as="span">
            <Text as="b">Task:</Text> {taskId}
          </Text>
          <Text as="span">
            <Text as="b">DAG:</Text> {dagId}
          </Text>
        </HStack>
      </Box>

      {error && (
        <Box
          bg="red.subtle"
          borderColor="red.emphasized"
          borderRadius="lg"
          borderWidth="1px"
          color="red.fg"
          fontSize="sm"
          mb={4}
          p={3}
        >
          {error}
        </Box>
      )}

      {modelName && (
        <Badge borderRadius="full" colorPalette="brand" fontSize="sm" mb={5} px={3} py={1}>
          {modelName}
        </Badge>
      )}

      <SimpleGrid columns={2} gap={4} maxW="640px">
        <StatBox label="Requests" value={usage.requests} />
        <StatBox label="Tool calls" value={usage.tool_calls} />
        <StatBox label="Input tokens" value={usage.input_tokens} />
        <StatBox label="Output tokens" value={usage.output_tokens} />
        <StatBox label="Total tokens" value={usage.total_tokens} />
        <StatBox label="Cost" tooltip={COST_TOOLTIP} value={usage.cost === null ? "—" : `$${usage.cost}`} />
      </SimpleGrid>
    </Box>
  );
};
