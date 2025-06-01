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
import { Button, Field, Heading, HStack, VStack, Text } from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { FiPlay } from "react-icons/fi";

import {
  useAssetServiceCreateAssetEvent,
  UseAssetServiceGetAssetEventsKeyFn,
  useAssetServiceMaterializeAsset,
  UseDagRunServiceGetDagRunsKeyFn,
  useDagServiceGetDagDetails,
  useDagServiceRecentDagRunsKey,
  useDependenciesServiceGetDependencies,
  UseGridServiceGridDataKeyFn,
  UseTaskInstanceServiceGetTaskInstancesKeyFn,
} from "openapi/queries";
import type {
  AssetEventResponse,
  AssetResponse,
  DAGRunResponse,
  EdgeResponse,
} from "openapi/requests/types.gen";
import { ErrorAlert } from "src/components/ErrorAlert";
import { JsonEditor } from "src/components/JsonEditor";
import { Dialog, toaster } from "src/components/ui";
import { Checkbox } from "src/components/ui/Checkbox";
import { RadioCardItem, RadioCardRoot } from "src/components/ui/RadioCard";
import { useTogglePause } from "src/queries/useTogglePause";

type Props = {
  readonly asset: AssetResponse;
  readonly onClose: () => void;
  readonly open: boolean;
};

export const CreateAssetEventModal = ({ asset, onClose, open }: Props) => {
  const [eventType, setEventType] = useState("manual");
  const [extraError, setExtraError] = useState<string | undefined>();
  const [unpause, setUnpause] = useState(true);
  const [extra, setExtra] = useState("{}");
  const queryClient = useQueryClient();

  const { data } = useDependenciesServiceGetDependencies({ nodeId: `asset:${asset.id}` }, undefined, {
    enabled: Boolean(asset) && Boolean(asset.id),
  });

  const upstreamDags: Array<EdgeResponse> = (data?.edges ?? []).filter(
    (edge) => edge.target_id === `asset:${asset.id}` && edge.source_id.startsWith("dag:"),
  );
  const hasUpstreamDag = upstreamDags.length === 1;
  const [upstreamDag] = upstreamDags;
  const upstreamDagId = hasUpstreamDag ? upstreamDag?.source_id.replace("dag:", "") : undefined;

  // TODO move validate + prettify into JsonEditor
  const validateAndPrettifyJson = (newValue: string) => {
    try {
      const parsedJson = JSON.parse(newValue) as JSON;

      setExtraError(undefined);

      const formattedJson = JSON.stringify(parsedJson, undefined, 2);

      if (formattedJson !== extra) {
        setExtra(formattedJson); // Update only if the value is different
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error occurred.";

      setExtraError(errorMessage);
    }
  };

  const onSuccess = async (response: AssetEventResponse | DAGRunResponse) => {
    setExtra("{}");
    setExtraError(undefined);
    onClose();

    let queryKeys = [UseAssetServiceGetAssetEventsKeyFn({ assetId: asset.id }, [{ assetId: asset.id }])];

    if ("dag_run_id" in response) {
      const dagId = response.dag_id;

      queryKeys = [
        ...queryKeys,
        [useDagServiceRecentDagRunsKey],
        UseDagRunServiceGetDagRunsKeyFn({ dagId }, [{ dagId }]),
        UseTaskInstanceServiceGetTaskInstancesKeyFn({ dagId, dagRunId: "~" }, [{ dagId, dagRunId: "~" }]),
        UseGridServiceGridDataKeyFn({ dagId }, [{ dagId }]),
      ];

      toaster.create({
        description: `Upstream Dag ${response.dag_id} was triggered successfully.`,
        title: "Materializing Asset",
        type: "success",
      });
    } else {
      toaster.create({
        description: "Manual asset event creation was successful.",
        title: "Asset Event Created",
        type: "success",
      });
    }

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));
  };

  const { data: dag } = useDagServiceGetDagDetails({ dagId: upstreamDagId ?? "" }, undefined, {
    enabled: Boolean(upstreamDagId),
  });

  const { mutate: togglePause } = useTogglePause({ dagId: dag?.dag_id ?? upstreamDagId ?? "" });

  const {
    error: manualError,
    isPending,
    mutate: createAssetEvent,
  } = useAssetServiceCreateAssetEvent({ onSuccess });
  const {
    error: materializeError,
    isPending: isMaterializePending,
    mutate: materializeAsset,
  } = useAssetServiceMaterializeAsset({
    onSuccess,
  });

  const handleSubmit = () => {
    if (eventType === "materialize") {
      if (unpause && dag?.is_paused) {
        togglePause({
          dagId: dag.dag_id,
          requestBody: {
            is_paused: false,
          },
        });
      }
      materializeAsset({ assetId: asset.id });
    } else {
      createAssetEvent({
        requestBody: { asset_id: asset.id, extra: JSON.parse(extra) as Record<string, unknown> },
      });
    }
  };

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="xl" unmountOnExit>
      <Dialog.Content backdrop>
        <Dialog.Header paddingBottom={0}>
          <VStack align="start" gap={4}>
            <Heading size="xl">Create Asset Event for {asset.name}</Heading>
          </VStack>
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body>
          <RadioCardRoot
            mb={6}
            onChange={(event) => {
              setEventType((event.target as HTMLInputElement).value);
            }}
            value={eventType}
          >
            <HStack align="stretch">
              <RadioCardItem
                description={`Trigger the Dag upstream of this asset${upstreamDagId === undefined ? "" : `: ${dag?.dag_display_name ?? upstreamDagId}`}`}
                disabled={!hasUpstreamDag}
                label="Materialize"
                value="materialize"
              />
              <RadioCardItem description="Directly create an Asset Event" label="Manual" value="manual" />
            </HStack>
          </RadioCardRoot>
          {eventType === "manual" ? (
            <Field.Root mt={6}>
              <Field.Label fontSize="md">Asset Event Extra</Field.Label>
              <JsonEditor onChange={validateAndPrettifyJson} value={extra} />
              <Text color="fg.error">{extraError}</Text>
            </Field.Root>
          ) : undefined}
          {eventType === "materialize" && dag?.is_paused ? (
            <Checkbox checked={unpause} colorPalette="blue" onChange={() => setUnpause(!unpause)}>
              Unpause {dag.dag_display_name} on trigger
            </Checkbox>
          ) : undefined}
          <ErrorAlert error={eventType === "manual" ? manualError : materializeError} />
        </Dialog.Body>
        <Dialog.Footer>
          <Button
            colorPalette="blue"
            disabled={Boolean(extraError)}
            loading={isPending || isMaterializePending}
            onClick={handleSubmit}
          >
            <FiPlay /> Create Event
          </Button>
        </Dialog.Footer>
      </Dialog.Content>
    </Dialog.Root>
  );
};
