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
import { Box, createListCollection, Flex, Heading, Stack, Text } from "@chakra-ui/react";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";

import type { Direction } from "src/components/Graph/DirectionDropdown";
import type { DefaultTaskInstanceTab } from "src/constants/tab";
import {
  useClearPreventRunningTaskDefault,
  useClearRunDefaultOptions,
  useClearTaskInstanceDefaultOptions,
  useDefaultGraphDirection,
  useDefaultLandingPage,
  useDefaultTaskInstanceTab,
  useMarkTaskInstanceDefaultOptions,
  type LandingPageOption,
} from "src/hooks/useUserSettings";
import { Select } from "src/system-components";
import SegmentedControl from "src/system-components/SegmentedControl";
import { Switch } from "src/system-components/Switch";
import { useDocumentTitle } from "src/utils";
import type { Option } from "src/utils/option";

type SelectOption<T extends string> = { label: string; value: T };

const SettingRow = ({
  control,
  helper,
  label,
}: {
  readonly control: ReactNode;
  readonly helper?: string;
  readonly label: string;
}) => (
  <Flex align="center" gap={6} justifyContent="space-between" py={3}>
    <Box>
      <Text fontWeight="medium">{label}</Text>
      {helper === undefined ? undefined : (
        <Text color="fg.muted" fontSize="sm">
          {helper}
        </Text>
      )}
    </Box>
    {control}
  </Flex>
);

const SelectSetting = <T extends string>({
  helper,
  label,
  onChange,
  options,
  testId,
  value,
}: {
  readonly helper?: string;
  readonly label: string;
  readonly onChange: (value: T) => void;
  readonly options: Array<SelectOption<T>>;
  readonly testId: string;
  readonly value: T;
}) => {
  const collection = createListCollection({ items: options });

  return (
    <SettingRow
      control={
        <Select.Root
          collection={collection}
          onValueChange={(event) => {
            const [next] = event.value;

            if (next !== undefined) {
              onChange(next as T);
            }
          }}
          value={[value]}
          width="220px"
        >
          <Select.Trigger dataTestId={testId}>
            <Select.ValueText />
          </Select.Trigger>
          <Select.Content>
            {options.map((option) => (
              <Select.Item item={option} key={option.value}>
                {option.label}
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
      }
      helper={helper}
      label={label}
    />
  );
};

const ToggleSetting = ({
  defaultValues,
  helper,
  label,
  multiple = false,
  onChange,
  options,
}: {
  readonly defaultValues: Array<string>;
  readonly helper?: string;
  readonly label: string;
  readonly multiple?: boolean;
  readonly onChange: (values: Array<string>) => void;
  readonly options: Array<Option>;
}) => (
  <Box py={3}>
    <Text fontWeight="medium">{label}</Text>
    {helper === undefined ? undefined : (
      <Text color="fg.muted" fontSize="sm" mb={2}>
        {helper}
      </Text>
    )}
    <SegmentedControl
      defaultValues={defaultValues}
      multiple={multiple}
      onChange={onChange}
      options={options}
    />
  </Box>
);

const Section = ({ children, title }: { readonly children: ReactNode; readonly title: string }) => (
  <Box>
    <Heading
      borderBottomWidth="1px"
      color="fg.muted"
      fontSize="sm"
      fontWeight="bold"
      letterSpacing="wider"
      mb={1}
      pb={2}
      textTransform="uppercase"
    >
      {title}
    </Heading>
    <Stack divideY="1px" gap={0}>
      {children}
    </Stack>
  </Box>
);

export const Settings = () => {
  const { t: translate } = useTranslation(["common", "components", "dags", "dag"]);

  useDocumentTitle(translate("settings.title"));

  const [graphDirection, setGraphDirection] = useDefaultGraphDirection();
  const [clearRunOptions, setClearRunOptions] = useClearRunDefaultOptions();
  const [clearTaskOptions, setClearTaskOptions] = useClearTaskInstanceDefaultOptions();
  const [preventRunningTask, setPreventRunningTask] = useClearPreventRunningTaskDefault();
  const [markTaskOptions, setMarkTaskOptions] = useMarkTaskInstanceDefaultOptions();
  const [defaultTaskInstanceTab, setDefaultTaskInstanceTab] = useDefaultTaskInstanceTab();
  const [defaultLandingPage, setDefaultLandingPage] = useDefaultLandingPage();

  const taskInstanceTabOptions: Array<SelectOption<DefaultTaskInstanceTab>> = [
    { label: translate("dag:tabs.logs"), value: "logs" },
    { label: translate("dag:tabs.details"), value: "details" },
    { label: translate("dag:tabs.renderedTemplates"), value: "rendered_templates" },
    { label: translate("dag:tabs.code"), value: "code" },
    { label: translate("dag:tabs.auditLog"), value: "events" },
    { label: translate("dag:tabs.assetEvents"), value: "asset_events" },
    { label: translate("dag:tabs.xcom"), value: "xcom" },
  ];

  const landingPageOptions: Array<SelectOption<LandingPageOption>> = [
    { label: translate("settings.general.landingPage.options.dashboard"), value: "dashboard" },
    { label: translate("settings.general.landingPage.options.dags"), value: "dags" },
  ];

  const directionOptions: Array<SelectOption<Direction>> = [
    { label: translate("components:graph.directionRight"), value: "RIGHT" },
    { label: translate("components:graph.directionLeft"), value: "LEFT" },
    { label: translate("components:graph.directionUp"), value: "UP" },
    { label: translate("components:graph.directionDown"), value: "DOWN" },
  ];

  const clearRunToggleOptions: Array<Option> = [
    { label: translate("dags:runAndTaskActions.options.existingTasks"), value: "existingTasks" },
    { label: translate("dags:runAndTaskActions.options.onlyFailed"), value: "onlyFailed" },
    { label: translate("dags:runAndTaskActions.options.queueNew"), value: "newTasks" },
  ];

  const directionalToggleOptions: Array<Option> = [
    { label: translate("dags:runAndTaskActions.options.past"), value: "past" },
    { label: translate("dags:runAndTaskActions.options.future"), value: "future" },
    { label: translate("dags:runAndTaskActions.options.upstream"), value: "upstream" },
    { label: translate("dags:runAndTaskActions.options.downstream"), value: "downstream" },
  ];

  return (
    <Box maxW="720px">
      <Heading mb={1} size="lg">
        {translate("settings.title")}
      </Heading>
      <Text color="fg.muted" mb={6}>
        {translate("settings.description")}
      </Text>
      <Stack gap={8}>
        <Section title={translate("settings.general.title")}>
          <SelectSetting
            helper={translate("settings.general.landingPage.helper")}
            label={translate("settings.general.landingPage.label")}
            onChange={setDefaultLandingPage}
            options={landingPageOptions}
            testId="default-landing-page"
            value={defaultLandingPage}
          />
        </Section>
        <Section title={translate("settings.graph.title")}>
          <SelectSetting
            helper={translate("settings.graph.defaultDirection.helper")}
            label={translate("settings.graph.defaultDirection.label")}
            onChange={setGraphDirection}
            options={directionOptions}
            testId="default-graph-direction"
            value={graphDirection}
          />
        </Section>
        <Section title={translate("settings.clearing.title")}>
          <ToggleSetting
            defaultValues={clearRunOptions}
            helper={translate("settings.clearing.runSelection.helper")}
            label={translate("settings.clearing.runSelection.label")}
            onChange={setClearRunOptions}
            options={clearRunToggleOptions}
          />
          <ToggleSetting
            defaultValues={clearTaskOptions}
            helper={translate("settings.clearing.taskSelection.helper")}
            label={translate("settings.clearing.taskSelection.label")}
            multiple
            onChange={setClearTaskOptions}
            options={[
              ...directionalToggleOptions,
              { label: translate("dags:runAndTaskActions.options.onlyFailed"), value: "onlyFailed" },
            ]}
          />
          <SettingRow
            control={
              <Switch
                checked={preventRunningTask}
                data-testid="clear-prevent-running-task"
                onCheckedChange={(event) => setPreventRunningTask(event.checked)}
              />
            }
            helper={translate("settings.clearing.preventRunningTask.helper")}
            label={translate("settings.clearing.preventRunningTask.label")}
          />
        </Section>
        <Section title={translate("settings.marking.title")}>
          <ToggleSetting
            defaultValues={markTaskOptions}
            helper={translate("settings.marking.taskSelection.helper")}
            label={translate("settings.marking.taskSelection.label")}
            multiple
            onChange={setMarkTaskOptions}
            options={directionalToggleOptions}
          />
        </Section>
        <Section title={translate("settings.taskInstance.title")}>
          <SelectSetting
            helper={translate("settings.taskInstance.defaultTab.helper")}
            label={translate("settings.taskInstance.defaultTab.label")}
            onChange={setDefaultTaskInstanceTab}
            options={taskInstanceTabOptions}
            testId="default-task-instance-tab"
            value={defaultTaskInstanceTab}
          />
        </Section>
      </Stack>
    </Box>
  );
};
