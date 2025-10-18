import { useEffect, useState } from "react";
import { VStack, Icon, Text, Spinner } from "@chakra-ui/react";
import { GoAlertFill } from "react-icons/go";
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
  readonly preventRunningTask: boolean;
};

const ClearTaskInstanceConfirmationDialog = ({
  onClose,
  open,
  onConfirm,
  dagDetails,
  preventRunningTask,
}: Props) => {
  const { t: translate } = useTranslation();
  const { data, isFetching } = useClearTaskInstancesDryRun({
    dagId: dagDetails?.dagId ?? "",
    options: {
      enabled: open && !!dagDetails,
      refetchOnMount: "always",
      refetchOnWindowFocus: false,
      staleTime: 0,
      gcTime: 0,
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

  const handleConfirm = () => {
    if (onConfirm) onConfirm();
    onClose();
  };

  const taskCurrentState = data?.task_instances?.[0]?.state;

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
          <VStack align="center" justify="center" py={8} gap={3}>
            <Spinner size="lg" />
            <Text fontSize="md" color="gray.600">
              {translate("common:loadingTaskDetails", "Loading task details…")}
            </Text>
          </VStack>
        ) : isReady ? (
          <>
            <Dialog.Header>
              <VStack align="start" gap={4}>
                <Dialog.Title>
                  <Icon size="md" color="tomato">
                    <GoAlertFill />
                  </Icon>
                  {translate("dags:runAndTaskActions.confirmationDialog.title")}
                </Dialog.Title>
                <Dialog.Description>
                  {data?.task_instances?.[0] && (
                    <>
                      {translate(
                        "dags:runAndTaskActions.confirmationDialog.description",
                        {
                          state: taskCurrentState,
                          time:
                            data.task_instances[0].start_date &&
                            getRelativeTime(
                              data.task_instances[0].start_date
                            ),
                          user:
                            data.task_instances[0].unixname ?? "unknown user",
                        }
                      )}
                    </>
                  )}
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
