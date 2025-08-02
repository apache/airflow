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
import { HStack, Text, Box } from "@chakra-ui/react";
import { useCallback, useState, useRef } from "react";
import { FiBarChart, FiMessageSquare } from "react-icons/fi";

import type { DAGRunResponse } from "openapi/requests/types.gen";
import { ClearRunButton } from "src/components/Clear";
import { DagVersion } from "src/components/DagVersion";
import EditableMarkdownButton from "src/components/EditableMarkdownButton";
import { HeaderCard } from "src/components/HeaderCard";
import { LimitedItemsList } from "src/components/LimitedItemsList";
import { MarkRunAsButton } from "src/components/MarkAs";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import Time from "src/components/Time";
import { usePatchDagRun } from "src/queries/usePatchDagRun";
import { getDuration, useContainerWidth } from "src/utils";

export const Header = ({
  dagRun,
  isRefreshing,
}: {
  readonly dagRun: DAGRunResponse;
  readonly isRefreshing?: boolean;
}) => {
  const [note, setNote] = useState<string | null>(dagRun.note);

  const dagId = dagRun.dag_id;
  const dagRunId = dagRun.dag_run_id;

  const { isPending, mutate } = usePatchDagRun({
    dagId,
    dagRunId,
  });

  const onConfirm = useCallback(() => {
    if (note !== dagRun.note) {
      mutate({
        dagId,
        dagRunId,
        requestBody: { note },
      });
    }
  }, [dagId, dagRun.note, dagRunId, mutate, note]);

  const onOpen = () => {
    setNote(dagRun.note ?? "");
  };

  const containerRef = useRef<HTMLDivElement>();
  const containerWidth = useContainerWidth(containerRef);

  return (
    <Box ref={containerRef}>
      <HeaderCard
        actions={
          <>
            <EditableMarkdownButton
              header="Dag Run Note"
              icon={<FiMessageSquare />}
              isPending={isPending}
              mdContent={note}
              onConfirm={onConfirm}
              onOpen={onOpen}
              placeholder="Add a note..."
              setMdContent={setNote}
              text={Boolean(dagRun.note) ? "Note" : "Add a note"}
              withText={containerWidth > 700}
            />
            <ClearRunButton dagRun={dagRun} withText={containerWidth > 700} />
            <MarkRunAsButton dagRun={dagRun} withText={containerWidth > 700} />
          </>
        }
        icon={<FiBarChart />}
        isRefreshing={isRefreshing}
        state={dagRun.state}
        stats={[
          ...(dagRun.logical_date === null
            ? []
            : [
                {
                  label: "Logical Date",
                  value: <Time datetime={dagRun.logical_date} />,
                },
              ]),
          {
            label: "Run Type",
            value: (
              <HStack>
                <RunTypeIcon runType={dagRun.run_type} />
                <Text>{dagRun.run_type}</Text>
              </HStack>
            ),
          },
          { label: "Start", value: <Time datetime={dagRun.start_date} /> },
          { label: "End", value: <Time datetime={dagRun.end_date} /> },
          { label: "Duration", value: getDuration(dagRun.start_date, dagRun.end_date) },
          {
            label: "Dag Version(s)",
            value: (
              <LimitedItemsList
                items={dagRun.dag_versions.map((version) => (
                  <DagVersion key={version.id} version={version} />
                ))}
                maxItems={4}
              />
            ),
          },
        ]}
        title={<Time datetime={dagRun.run_after} />}
      />
    </Box>
  );
};
