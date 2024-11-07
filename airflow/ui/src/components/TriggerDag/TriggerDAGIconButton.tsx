import { Box } from "@chakra-ui/react";
import { FiPlay } from "react-icons/fi";
import { useDisclosure } from "@chakra-ui/react";
import TriggerDAGModal from "./TriggerDAGModal";
import type { TriggerDAGButtonProps } from "./TriggerDag";

const TriggerDAGIconButton: React.FC<TriggerDAGButtonProps> = ({ dagDisplayName, dagId }) => {
  const { onClose, onOpen, open } = useDisclosure();  

  return (
    <Box>
      <Box alignSelf="center" cursor="pointer" onClick={onOpen}>
        <FiPlay />
      </Box>

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
