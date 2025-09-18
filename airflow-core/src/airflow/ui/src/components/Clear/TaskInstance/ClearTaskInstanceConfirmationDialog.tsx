import { VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { Button, Dialog } from "src/components/ui";
import { useClearTaskInstancesDryRun } from "src/queries/useClearTaskInstancesDryRun";
import { getRelativeTime } from "src/utils/datetimeUtils";

type Props = {
  readonly onClose: () => void;
  readonly onConfirm?: () => void;
  readonly open: boolean;
  readonly dagDetails?: {
    dagId: string;
    dagRunId: string;
    taskId: string;
    mapIndex?: number;
    downstream?: boolean;
    future?: boolean;
    past?: boolean;
    upstream?: boolean;
    onlyFailed?: boolean;
  };
};

const ClearTaskInstanceConfirmationDialog = ({ onClose, open, onConfirm, dagDetails }: Props) => {
  const { t: translate } = useTranslation();
  const { data, isFetching } = useClearTaskInstancesDryRun({
    dagId: dagDetails?.dagId || "",
    options: {
      enabled: open && !!dagDetails,
      refetchOnMount: "always",
      refetchOnWindowFocus: false,
      staleTime: 0,
      gcTime: 0,
    },
    requestBody: {
      dag_run_id: dagDetails?.dagRunId,
      include_downstream: dagDetails?.downstream,
      include_future: dagDetails?.future,
      include_past: dagDetails?.past,
      include_upstream: dagDetails?.upstream,
      only_failed: dagDetails?.onlyFailed,
      task_ids: [[dagDetails?.taskId || "", dagDetails?.mapIndex || 0]],
    },
  });

  // Shared confirm handler to reduce duplication
  const handleConfirm = () => {
    if (onConfirm) {
      onConfirm();
    }
    onClose();
  };

  if (isFetching) {
    return null;
  }

  // Dialog will only open if the task is in a 'running' state
  // Added up-for-retry and restarting
  const taskCurrentState = !isFetching && data && data.task_instances?.[0]?.state
  if (taskCurrentState != "running" && taskCurrentState != "restarting" && taskCurrentState != "up_for_retry") {
    handleConfirm();
    return null;
  }

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open}>
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align={"start"} gap={4}>
            <Dialog.Title>{translate("dags:runAndTaskActions.clearTask.title")}</Dialog.Title>
            <Dialog.Description>
              {data?.task_instances?.[0] && (
                <>
                  {translate("dags:runAndTaskActions.clearTask.lastRun")}{" "}
                  <strong>{data.task_instances[0].unixname || "unknown user"}</strong>{" "}
                  {data.task_instances[0].start_date && (
                    <>{translate("dags:runAndTaskActions.clearTask.starting")} {getRelativeTime(data.task_instances[0].start_date)}</>
                  )}
                  {data.task_instances[0].end_date && data.task_instances[0].state === "success" && (
                    <> {translate("dags:runAndTaskActions.clearTask.completed")} {getRelativeTime(data.task_instances[0].end_date)}</>
                  )}
                  . {translate("dags:runAndTaskActions.clearTask.state")} <strong>{data.task_instances[0].state || "unknown"}</strong>. {translate("dags:runAndTaskActions.clearTask.clearAgain")}
                </>
              )}
            </Dialog.Description>
          </VStack>
        </Dialog.Header>
        <Dialog.Footer>
          <Button colorPalette="blue" onClick={onClose}>
            Cancel
          </Button>
          <Button colorPalette="red" onClick={handleConfirm}>
            Confirm
          </Button>
        </Dialog.Footer>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default ClearTaskInstanceConfirmationDialog;
