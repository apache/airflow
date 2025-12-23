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
import { Input, Field, Stack } from "@chakra-ui/react";
import { Controller, type Control } from "react-hook-form";
import { useTranslation } from "react-i18next";

import type { DagRunTriggerParams } from "src/utils/trigger";

import EditableMarkdown from "./EditableMarkdown";

type TriggerDAGAdvancedOptionsProps = {
  readonly control: Control<DagRunTriggerParams>;
};

const TriggerDAGAdvancedOptions = ({ control }: TriggerDAGAdvancedOptionsProps) => {
  const { t: translate } = useTranslation(["common", "components"]);
  const { t: rootTranslate } = useTranslation();

  return (
    <>
      <Controller
        control={control}
        name="dagRunId"
        render={({ field }) => (
          <Field.Root mt={6} orientation="horizontal">
            <Stack>
              <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
                {translate("runId")}
              </Field.Label>
            </Stack>
            <Stack css={{ flexBasis: "70%" }}>
              <Input {...field} size="sm" />
              <Field.HelperText>{translate("components:triggerDag.runIdHelp")}</Field.HelperText>
            </Stack>
          </Field.Root>
        )}
      />

      <Controller
        control={control}
        name="partitionKey"
        render={({ field }) => (
          <Field.Root mt={6} orientation="horizontal">
            <Stack>
              <Field.Label fontSize="md" style={{ flexBasis: "30%" }}>
                {rootTranslate("dagRun.partitionKey")}
              </Field.Label>
            </Stack>
            <Stack css={{ flexBasis: "70%" }}>
              <Input {...field} size="sm" />
            </Stack>
          </Field.Root>
        )}
      />
      <Controller
        control={control}
        name="note"
        render={({ field }) => (
          <Field.Root mt={6}>
            <Field.Label fontSize="md">{translate("note.dagRun")}</Field.Label>
            <EditableMarkdown field={field} placeholder={translate("note.placeholder")} />
          </Field.Root>
        )}
      />
    </>
  );
};

export default TriggerDAGAdvancedOptions;
