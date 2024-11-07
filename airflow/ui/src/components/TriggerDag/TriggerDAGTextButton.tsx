import { Box, Button } from "@chakra-ui/react";
import { FiPlay } from "react-icons/fi";
import { useDisclosure } from "@chakra-ui/react";
import TriggerDAGModal from "./TriggerDAGModal";
import type { TriggerDAGButtonProps } from "./TriggerDag";

const TriggerDAGIconButton: React.FC<TriggerDAGButtonProps> = ({ dagDisplayName, dagId }) => {
  const { onClose, onOpen, open } = useDisclosure();  

  return (
    <Box>
        <Button colorPalette="blue" onClick={onOpen}>
            <FiPlay />
            Trigger
        </Button>

      <TriggerDAGModal
        dagDisplayName={dagDisplayName}
        dagId={dagId}
        onClose={onClose}
        open={open}
      />
    </Box>
  );
};

export default TriggerDAGIconButton;
