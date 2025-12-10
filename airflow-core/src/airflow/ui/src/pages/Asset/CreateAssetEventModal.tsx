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
import { Button, CloseButton, Dialog, Field, Heading, HStack, Text, VStack } from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiPlay } from "react-icons/fi";

import {
  useAssetServiceCreateAssetEvent,
  UseAssetServiceGetAssetEventsKeyFn,
  useAssetServiceMaterializeAsset,
  UseDagRunServiceGetDagRunsKeyFn,
  useDagServiceGetDagDetails,
  useDagServiceGetDagsUiKey,
  useDependenciesServiceGetDependencies,
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
import { toaster } from "src/components/ui";
import { Checkbox } from "src/components/ui/Checkbox";
import { RadioCardItem, RadioCardRoot } from "src/components/ui/RadioCard";
import { useTogglePause } from "src/queries/useTogglePause";

type Props = {
  readonly asset: AssetResponse;
  readonly onClose: () => void;
  readonly open: boolean;
};

export const CreateAssetEventModal = ({ asset, onClose, open }: Props) => {
  const { t: translate } = useTranslation(["assets", "components"]);
  const [eventType, setEventType] = useState("manual");
  const [extraError, setExtraError] = useState<string | undefined>();
  const [unpause, setUnpause] = useState(true);
  const [extra, setExtra] = useState("{}");
  const [partitionKey, setPartitionKey] = useState<string | undefined>(undefined);
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
      const errorMessage = error instanceof Error ? error.message : translate("common:error.unknown");

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
        [useDagServiceGetDagsUiKey],
        UseDagRunServiceGetDagRunsKeyFn({ dagId }, [{ dagId }]),
        UseTaskInstanceServiceGetTaskInstancesKeyFn({ dagId, dagRunId: "~" }, [{ dagId, dagRunId: "~" }]),
      ];

      toaster.create({
        description: translate("createEvent.success.materializeDescription", { dagId: response.dag_id }),
        title: translate("createEvent.success.materializeTitle"),
        type: "success",
      });
    } else {
      toaster.create({
        description: translate("createEvent.success.manualDescription"),
        title: translate("createEvent.success.manualTitle"),
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
        requestBody: {
          asset_id: asset.id,
          extra: JSON.parse(extra) as Record<string, unknown>,
          partition_key: partitionKey ?? null,
        },
      });
    }
  };

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open} size="xl" unmountOnExit>
      <Dialog.Backdrop />
      <Dialog.Positioner>
        <Dialog.Content>
          <Dialog.Header paddingBottom={0}>
            <VStack align="start" gap={4}>
              <Heading size="xl">{translate("createEvent.title", { name: asset.name })}</Heading>
            </VStack>
          </Dialog.Header>

          <Dialog.CloseTrigger asChild position="absolute" right="2" top="2">
            <CloseButton size="sm" />
          </Dialog.CloseTrigger>

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
                  description={
                    upstreamDagId === undefined
                      ? translate("createEvent.materialize.description")
                      : translate("createEvent.materialize.descriptionWithDag", {
                          dagName: dag?.dag_display_name ?? upstreamDagId,
                        })
                  }
                  disabled={!hasUpstreamDag}
                  label={translate("createEvent.materialize.label")}
                  value="materialize"
                />
                <RadioCardItem
                  description={translate("createEvent.manual.description")}
                  label={translate("createEvent.manual.label")}
                  value="manual"
                />
              </HStack>
            </RadioCardRoot>
            {eventType === "manual" ? (
              <>
                <Field.Root mt={6}>
                  <Field.Label fontSize="md">{translate("createEvent.manual.extra")}</Field.Label>
                  <JsonEditor onChange={validateAndPrettifyJson} value={extra} />
                  <Text color="fg.error">{extraError}</Text>
                </Field.Root>
                <Field.Root mt={6}>
                  <Field.Label fontSize="md">{translate("dagRun.partitionKey")}</Field.Label>
                  <JsonEditor onChange={setPartitionKey} value={partitionKey} />
                  <Text color="fg.error">{extraError}</Text>
                </Field.Root>
              </>
            ) : undefined}
            {eventType === "materialize" && dag?.is_paused ? (
              <Checkbox checked={unpause} colorPalette="brand" onChange={() => setUnpause(!unpause)}>
                {translate("createEvent.materialize.unpauseDag", { dagName: dag.dag_display_name })}
              </Checkbox>
            ) : undefined}
            <ErrorAlert error={eventType === "manual" ? manualError : materializeError} />
          </Dialog.Body>
          <Dialog.Footer>
            <Button
              colorPalette="brand"
              disabled={Boolean(extraError)}
              loading={isPending || isMaterializePending}
              onClick={handleSubmit}
            >
              <FiPlay /> {translate("createEvent.button")}
            </Button>
          </Dialog.Footer>
        </Dialog.Content>
      </Dialog.Positioner>
    </Dialog.Root>
  );
};
