import { Popover, Portal, Select, VStack } from "@chakra-ui/react";
import type { SelectValueChangeDetails } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { MdSettings } from "react-icons/md";
import { useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { DagVersionSelect } from "src/components/DagVersionSelect";
import { DirectionDropdown } from "src/components/Graph/DirectionDropdown";
import { IconButton } from "src/components/ui";
import { dependenciesKey } from "src/constants/localStorage";

const deps = ["all", "immediate", "tasks"];

type Dependency = (typeof deps)[number];

export const GraphSettings = () => {
  const { t: translate } = useTranslation(["common", "dag"]);
  const { dagId = "" } = useParams();
  const [dependencies, setDependencies, removeDependencies] = useLocalStorage<Dependency>(
    dependenciesKey(dagId),
    "tasks",
  );

  const handleDepsChange = (event: SelectValueChangeDetails<{ label: string; value: Array<string> }>) => {
    if (event.value[0] === undefined || event.value[0] === "tasks" || !deps.includes(event.value[0])) {
      removeDependencies();
    } else {
      setDependencies(event.value[0]);
    }
  };

  const options = [
    { label: translate("dag:panel.dependencies.options.onlyTasks"), value: "tasks" },
    { label: translate("dag:panel.dependencies.options.externalConditions"), value: "immediate" },
    { label: translate("dag:panel.dependencies.options.allDagDependencies"), value: "all" },
  ];

  return (
    <Popover.Root autoFocus={false} positioning={{ placement: "bottom-end" }}>
      <Popover.Trigger asChild>
        <IconButton label={translate("dag:panel.buttons.options")}>
          <MdSettings />
        </IconButton>
      </Popover.Trigger>
      <Portal>
        <Popover.Positioner>
          <Popover.Content>
            <Popover.Arrow />
            <Popover.Body display="flex" flexDirection="column" gap={4} maxH="70vh" overflowY="auto" p={2}>
              <DagVersionSelect />

              <Select.Root
                // @ts-expect-error The expected option type is incorrect
                collection={{ items: options }}
                data-testid="dependencies"
                onValueChange={handleDepsChange}
                size="sm"
                value={[dependencies]}
              >
                <Select.Label fontSize="xs">{translate("dag:panel.dependencies.label")}</Select.Label>
                <Select.Control>
                  <Select.Trigger>
                    <Select.ValueText placeholder={translate("dag:panel.dependencies.label")} />
                  </Select.Trigger>
                  <Select.IndicatorGroup>
                    <Select.Indicator />
                  </Select.IndicatorGroup>
                </Select.Control>
                <Select.Positioner>
                  <Select.Content>
                    {options.map((option) => (
                      <Select.Item item={option} key={option.value}>
                        {option.label}
                      </Select.Item>
                    ))}
                  </Select.Content>
                </Select.Positioner>
              </Select.Root>

              <DirectionDropdown graphId={dagId} />
            </Popover.Body>
          </Popover.Content>
        </Popover.Positioner>
      </Portal>
    </Popover.Root>
  );
};
