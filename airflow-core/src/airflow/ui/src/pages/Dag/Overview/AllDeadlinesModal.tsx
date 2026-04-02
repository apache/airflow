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
import { Heading, HStack, Separator, Skeleton, VStack } from "@chakra-ui/react";
import { useState } from "react";

import { useDeadlinesServiceGetDeadlines } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import { Dialog } from "src/components/ui";
import { Pagination } from "src/components/ui/Pagination";

import { DeadlineRow } from "./DeadlineRow";

const PAGE_LIMIT = 10;

type AllDeadlinesModalProps = {
  readonly dagId: string;
  readonly endDate: string;
  readonly missed: boolean;
  readonly onClose: () => void;
  readonly open: boolean;
  readonly refetchInterval: number | false;
  readonly startDate: string;
  readonly title: string;
};

export const AllDeadlinesModal = ({
  dagId,
  endDate,
  missed,
  onClose,
  open,
  refetchInterval,
  startDate,
  title,
}: AllDeadlinesModalProps) => {
  const [page, setPage] = useState(1);
  const offset = (page - 1) * PAGE_LIMIT;

  const { data, error, isLoading } = useDeadlinesServiceGetDeadlines(
    missed
      ? {
          dagId,
          dagRunId: "~",
          lastUpdatedAtGte: startDate,
          lastUpdatedAtLte: endDate,
          limit: PAGE_LIMIT,
          missed: true,
          offset,
          orderBy: ["-last_updated_at"],
        }
      : {
          dagId,
          dagRunId: "~",
          deadlineTimeGte: endDate,
          limit: PAGE_LIMIT,
          missed: false,
          offset,
          orderBy: ["deadline_time"],
        },
    undefined,
    { enabled: open, refetchInterval },
  );

  const deadlines = data?.deadlines ?? [];
  const totalEntries = data?.total_entries ?? 0;

  const onOpenChange = () => {
    setPage(1);
    onClose();
  };

  return (
    <Dialog.Root onOpenChange={onOpenChange} open={open} scrollBehavior="inside" size="sm">
      <Dialog.Content>
        <Dialog.Header>
          <Heading size="sm">{title}</Heading>
        </Dialog.Header>
        <Dialog.CloseTrigger />
        <Dialog.Body pb={2}>
          <ErrorAlert error={error} />
          {isLoading ? (
            <VStack>
              {Array.from({ length: PAGE_LIMIT }).map((_, idx) => (
                // eslint-disable-next-line react/no-array-index-key
                <Skeleton height="36px" key={idx} width="100%" />
              ))}
            </VStack>
          ) : (
            <VStack gap={0} separator={<Separator />}>
              {deadlines.map((dl) => (
                <DeadlineRow deadline={dl} key={dl.id} />
              ))}
            </VStack>
          )}
        </Dialog.Body>
        {totalEntries > PAGE_LIMIT ? (
          <Pagination.Root
            count={totalEntries}
            onPageChange={(event) => setPage(event.page)}
            p={3}
            page={page}
            pageSize={PAGE_LIMIT}
          >
            <HStack justify="center">
              <Pagination.PrevTrigger />
              <Pagination.Items />
              <Pagination.NextTrigger />
            </HStack>
          </Pagination.Root>
        ) : undefined}
      </Dialog.Content>
    </Dialog.Root>
  );
};
