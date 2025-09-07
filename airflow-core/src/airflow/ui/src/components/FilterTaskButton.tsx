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
import { Box } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { MdFilterList } from "react-icons/md";
import { useSearchParams, useParams } from "react-router-dom";

import { Menu } from "src/components/ui";
import { Button } from "src/components/ui";
import ActionButton from "src/components/ui/ActionButton";

const FILTER_PARAM = "task_filter";

const OPTIONS = [
  { buttonKey: "taskFilter.button_all", itemKey: "taskFilter.all", value: "all" },
  { buttonKey: "taskFilter.upstream", itemKey: "taskFilter.upstream", value: "upstream" },
  { buttonKey: "taskFilter.downstream", itemKey: "taskFilter.downstream", value: "downstream" },
  { buttonKey: "taskFilter.both", itemKey: "taskFilter.both", value: "both" },
] as const;

type FilterValue = (typeof OPTIONS)[number]["value"];

type Props = {
  readonly withText?: boolean;
};

const FilterTaskButton = ({ withText = true }: Props) => {
  const { t: translate } = useTranslation("components");
  const [searchParams, setSearchParams] = useSearchParams();
  const { taskId } = useParams<{ taskId?: string }>();

  const rawFilter = searchParams.get(FILTER_PARAM) as FilterValue | null;
  const currentFilter = rawFilter ?? "all";

  const isActiveForThisTask = currentFilter !== "all";

  const handleSelect = (value: FilterValue) => {
    const next = new URLSearchParams(searchParams.toString());

    if (value === "all") {
      next.delete(FILTER_PARAM);
    } else {
      if (taskId === undefined) {
        return; // optional: show tooltip instead
      }
      next.set(FILTER_PARAM, value);
    }

    setSearchParams(next, { replace: true });
  };

  const selectedOption = OPTIONS.find((opt) => opt.value === currentFilter) ?? OPTIONS[0];
  const buttonLabel = translate(selectedOption.buttonKey);

  return (
    <Box display="inline-block" position="relative">
      <Menu.Root positioning={{ gutter: 0, placement: "bottom" }}>
        <Menu.Trigger asChild>
          <Box position="relative">
            <ActionButton
              actionName={`${translate("taskFilter.action_prefix", { defaultValue: "Filter" })}: ${buttonLabel}`}
              flexDirection="row-reverse"
              icon={<MdFilterList />}
              text={buttonLabel}
              withText={withText}
            />
            {isActiveForThisTask ? (
              <Box
                bg="blue.500"
                borderRadius="full"
                height={2.5}
                position="absolute"
                right={1} // adjust to align correctly
                top={1} // adjust to align correctly
                width={2.5}
              />
            ) : undefined}
          </Box>
        </Menu.Trigger>

        <Menu.Content>
          {OPTIONS.map((option) => (
            <Menu.Item
              asChild
              disabled={currentFilter === option.value}
              key={option.value}
              value={option.value}
            >
              <Button
                justifyContent="start"
                onClick={() => handleSelect(option.value)}
                size="sm"
                variant="ghost"
                width="100%"
              >
                {translate(option.itemKey)}
              </Button>
            </Menu.Item>
          ))}
        </Menu.Content>
      </Menu.Root>
    </Box>
  );
};

export default FilterTaskButton;
