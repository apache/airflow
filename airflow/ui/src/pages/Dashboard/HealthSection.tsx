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
import { Skeleton, Tag, TagLabel, Text, Tooltip } from "@chakra-ui/react";

export const HealthSection = ({
  isLoading,
  latestHeartbeat,
  status,
  title,
}: {
  readonly isLoading: boolean;
  readonly latestHeartbeat?: null | string;
  readonly status?: null | string;
  readonly title: string;
}) => {
  if (isLoading) {
    return <Skeleton borderRadius="full" height={34} width={100} />;
  }

  return (
    <Tooltip
      hasArrow
      isDisabled={!Boolean(latestHeartbeat)}
      label={
        <div>
          <Text>Status: {status}</Text>
          <Text>Last Heartbeat: {latestHeartbeat}</Text>
        </div>
      }
      shouldWrapChildren
    >
      <Tag
        borderColor={status === "healthy" ? "success.100" : "error.100"}
        borderRadius="full"
        borderWidth={1}
        colorScheme={status === "healthy" ? "success" : "error"}
        size="lg"
      >
        <TagLabel>{title}</TagLabel>
      </Tag>
    </Tooltip>
  );
};
