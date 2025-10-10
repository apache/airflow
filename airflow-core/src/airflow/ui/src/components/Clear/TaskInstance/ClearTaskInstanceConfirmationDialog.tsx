import { VStack } from "@chakra-ui/react";

import { Button, Dialog } from "src/components/ui";
import { useClearTaskInstancesDryRun } from "src/queries/useClearTaskInstancesDryRun";

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

  // Only auto-confirm for failed tasks if we have fresh data (not loading)
  const taskCurrentState = !isFetching && data && data.task_instances?.[0]?.state

  if (prevent_running_task === false) {
    handleConfirm();
    return null
  }
  
  if (taskCurrentState !== "queued" && taskCurrentState !== "scheduled") {
    handleConfirm();
    return null;
  }

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open}>
      <Dialog.Content backdrop>
        <Dialog.Header>
          <VStack align={"start"} gap={4}>
            <Dialog.Title>Confirm Clear Task Instance</Dialog.Title>
            <Dialog.Description>
              {data?.task_instances?.[0] && (
                <>
                  Task is currently {taskCurrentState} state. 
                  You are unable to clear this task instance until you disable/ uncheck the "Prevent rerun of running tasks" option in the  clear task dialog.
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
