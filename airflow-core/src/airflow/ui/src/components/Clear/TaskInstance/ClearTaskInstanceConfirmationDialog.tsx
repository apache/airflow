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
  readonly prevent_running_task: boolean;
};

const ClearTaskInstanceConfirmationDialog = ({ onClose, open, onConfirm, dagDetails, prevent_running_task }: Props) => {
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

  const taskCurrentState = !isFetching && data && data.task_instances?.[0]?.state
  if (!prevent_running_task) {
    handleConfirm();
    return null;
  }
  
  if (taskCurrentState !== "queued" && taskCurrentState !== "scheduled" && taskCurrentState !== "running") {
    handleConfirm();
    return null;
  }

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open}>
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align={"start"} gap={4}>
            <Dialog.Title>{translate("dags:runAndTaskActions.confirmationDialog.title")}</Dialog.Title>
            <Dialog.Description>
              {data?.task_instances?.[0] && (
                <>
                  {translate("dags:runAndTaskActions.confirmationDialog.description", {
                    state: taskCurrentState,
                    time: data.task_instances[0].start_date && (getRelativeTime(data.task_instances[0].start_date)),
                    user: data.task_instances[0].unixname || "unknown user",
                  })}
                </>
              )}
            </Dialog.Description>
          </VStack>
        </Dialog.Header>
        <Dialog.Footer>
          <Button colorPalette="red" onClick={handleConfirm}>
            {translate("common:modal.confirm")}
          </Button>
        </Dialog.Footer>
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default ClearTaskInstanceConfirmationDialog;
