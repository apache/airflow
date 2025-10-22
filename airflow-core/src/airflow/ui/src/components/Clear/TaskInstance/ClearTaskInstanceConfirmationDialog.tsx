
import { useEffect, useState, useCallback } from "react";
import { VStack, Icon, Text, Spinner } from "@chakra-ui/react";
import { GoAlertFill } from "react-icons/go";
import { useTranslation } from "react-i18next";
import { Button, Dialog } from "src/components/ui";
import { useClearTaskInstancesDryRun } from "src/queries/useClearTaskInstancesDryRun";
import { getRelativeTime } from "src/utils/datetimeUtils";

type Props = {
  readonly dagDetails?: {
    dagId: string;
    dagRunId: string;
    downstream?: boolean;
    future?: boolean;
    mapIndex?: number;
    onlyFailed?: boolean;
    past?: boolean;
    taskId: string;
    upstream?: boolean;
  };
  readonly onClose: () => void;
  readonly onConfirm?: () => void;
  readonly open: boolean;
  readonly preventRunningTask: boolean;
};

const ClearTaskInstanceConfirmationDialog = ({
  dagDetails,
  onClose,
  onConfirm,
  open,
  preventRunningTask,
}: Props) => {
  const { t: translate } = useTranslation();
  const { data, isFetching } = useClearTaskInstancesDryRun({
    dagId: dagDetails?.dagId ?? "",
    options: {
      enabled: open && Boolean(dagDetails),
      gcTime: 0,
      refetchOnMount: "always",
      refetchOnWindowFocus: false,
      staleTime: 0,
    },
    requestBody: {
      dag_run_id: dagDetails?.dagRunId ?? "",
      include_downstream: dagDetails?.downstream,
      include_future: dagDetails?.future,
      include_past: dagDetails?.past,
      include_upstream: dagDetails?.upstream,
      only_failed: dagDetails?.onlyFailed,
      task_ids: [[dagDetails?.taskId ?? "", dagDetails?.mapIndex ?? 0]],
    },
  });

  const [isReady, setIsReady] = useState(false);

  const handleConfirm = useCallback(() => {
    if (onConfirm) onConfirm();
    onClose();
  }, [onConfirm, onClose]);

  const taskCurrentState =
  data && data.task_instances.length > 0
    ? data.task_instances[0].state
    : undefined;

  useEffect(() => {
    if (!isFetching && open && data) {
      const isInTriggeringState =
        taskCurrentState === "queued" || taskCurrentState === "scheduled";

      if (!preventRunningTask || !isInTriggeringState) {
        handleConfirm();
      } else {
        setIsReady(true);
      }
    }
  }, [isFetching, data, open, handleConfirm, taskCurrentState, preventRunningTask]);

  return (
    <Dialog.Root lazyMount onOpenChange={onClose} open={open}>
      <Dialog.Content backdrop>
        {isFetching ? (
          // Loading State — keeps the dialog mounted during fetch
          <VStack align="center" gap={3} justify="center" py={8}>
            <Spinner size="lg" />
            <Text color="fg.solid" fontSize="md">
              {translate("common:task.documentation")}
            </Text>
          </VStack>
        ) : isReady ? (
          <>
            <Dialog.Header>
              <VStack align="start" gap={4}>
                <Dialog.Title>
                  <Icon color="tomato" size="lg" pr='2'>
                    <GoAlertFill />
                  </Icon>
                  {translate("dags:runAndTaskActions.confirmationDialog.title")}
                </Dialog.Title>
                <Dialog.Description>
                  {data.task_instances.length > 0 ? (
                    <>
                      {translate("dags:runAndTaskActions.confirmationDialog.description", {
                        state: taskCurrentState,
                        time:
                          data.task_instances[0].start_date !== null
                            ? getRelativeTime(data.task_instances[0].start_date)
                            : undefined,
                        user:
                          data.task_instances[0].unixname !== null &&
                          data.task_instances[0].unixname.trim() !== ""
                            ? data.task_instances[0].unixname
                            : "unknown user",
                      })}
                    </>
                  ) : null}
                </Dialog.Description>
              </VStack>
            </Dialog.Header>
            <Dialog.Footer>
              <Button colorPalette="blue" onClick={onClose}>
                {translate("common:modal.confirm")}
              </Button>
            </Dialog.Footer>
          </>
        ) : null}
      </Dialog.Content>
    </Dialog.Root>
  );
};

export default ClearTaskInstanceConfirmationDialog;
