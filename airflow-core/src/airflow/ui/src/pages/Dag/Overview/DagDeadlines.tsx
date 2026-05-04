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
import { Box, Button, Flex, Heading, Separator, Skeleton, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiClock } from "react-icons/fi";

import { useDeadlinesServiceGetDagDeadlineAlerts } from "openapi/queries";
import type { DeadlineAlertResponse } from "openapi/requests/types.gen";
import { ErrorAlert } from "src/components/ErrorAlert";
import { useDeadlines } from "src/queries/useDeadlines";

import { AllDeadlinesModal } from "./AllDeadlinesModal";
import { DeadlineRow } from "./DeadlineRow";

const LIMIT = 10;

type DagDeadlinesProps = {
  readonly dagId: string;
};

export const DagDeadlines = ({ dagId }: DagDeadlinesProps) => {
  const { t: translate } = useTranslation("dag");
  const [modalOpen, setModalOpen] = useState(false);

  const { data, error, isLoading } = useDeadlines({ dagId, limit: LIMIT });

  const { data: alertData } = useDeadlinesServiceGetDagDeadlineAlerts({ dagId, limit: 100 });

  const alertMap = new Map<string, DeadlineAlertResponse>();

  for (const alert of alertData?.deadline_alerts ?? []) {
    alertMap.set(alert.id, alert);
  }

  const deadlines = data?.deadlines ?? [];
  const totalEntries = data?.total_entries ?? 0;
  const hasMore = totalEntries > LIMIT;

  if (!isLoading && error === null && deadlines.length === 0) {
    return undefined;
  }

  const getAlert = (alertId?: string | null) =>
    alertId !== undefined && alertId !== null ? alertMap.get(alertId) : undefined;

  return (
    <Box>
      <Flex alignItems="center" color="fg.muted" gap={1} mb={2}>
        <FiClock />
        <Heading ml={1} size="xs">
          {translate("overview.deadlines.title")}
        </Heading>
      </Flex>
      <ErrorAlert error={error} />
      <Box borderRadius="lg" borderWidth={1} overflow="hidden" p={3}>
        {isLoading ? (
          <VStack>
            {Array.from({ length: 3 }).map((_, idx) => (
              // eslint-disable-next-line react/no-array-index-key
              <Skeleton height="36px" key={idx} width="100%" />
            ))}
          </VStack>
        ) : (
          <VStack gap={0} separator={<Separator />}>
            {deadlines.map((deadline) => (
              <DeadlineRow alert={getAlert(deadline.alert_id)} deadline={deadline} key={deadline.id} />
            ))}
          </VStack>
        )}
        {hasMore ? (
          <Button mt={2} onClick={() => setModalOpen(true)} size="xs" variant="ghost" width="100%">
            {translate("overview.deadlines.showAll")}
          </Button>
        ) : undefined}
      </Box>

      <AllDeadlinesModal
        alertMap={alertMap}
        dagId={dagId}
        onClose={() => setModalOpen(false)}
        open={modalOpen}
      />
    </Box>
  );
};
