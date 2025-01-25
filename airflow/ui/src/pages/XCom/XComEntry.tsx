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
import { Skeleton, HStack, Text, useToast } from "@chakra-ui/react";
import { useXcomServiceGetXcomEntry } from "openapi/queries";
import type { XComResponseString } from "openapi/requests/types.gen";
import { ClipboardIconButton, ClipboardRoot } from "src/components/ui";

type XComEntryProps = {
  readonly dagId: string;
  readonly mapIndex: number;
  readonly runId: string;
  readonly taskId: string;
  readonly xcomKey: string;
};

export const XComEntry = ({ dagId, mapIndex, runId, taskId, xcomKey }: XComEntryProps) => {
  const { data, error, isLoading } = useXcomServiceGetXcomEntry<XComResponseString>({
    dagId,
    dagRunId: runId,
    mapIndex,
    stringify: true,
    taskId,
    xcomKey,
  });

  const toast = useToast();

  if (isLoading) {
    return (
      <Skeleton data-testid="skeleton" display="inline-block" height="10px" width={200} />
    );
  }

  if (error) {
    return <Text color="red.500">Error: {error.message}</Text>;
  }

  const handleCopy = () => {
    toast({
      title: "Copied to clipboard",
      description: "XCom value has been copied.",
      status: "success",
      duration: 2000,
      isClosable: true,
    });
  };

  return data?.value?.length ? (
    <HStack>
      <Text>{data?.value}</Text>
      <ClipboardRoot value={data?.value ?? ""}>
        <ClipboardIconButton onClick={handleCopy} />
      </ClipboardRoot>
    </HStack>
  ) : (
    <Text>No XCom data available</Text>
  );
};

