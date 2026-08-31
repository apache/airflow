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
import { Button, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { MdAdd } from "react-icons/md";

import { useAddPool } from "src/queries/useAddPool";
import { Modal, Toaster } from "src/system-components";

import PoolForm, { type PoolBody } from "./PoolForm";

const AddPoolButton = () => {
  const { t: translate } = useTranslation("admin");
  const { onClose, onOpen, open } = useDisclosure();
  const { addPool, error, isPending, setError } = useAddPool({
    onSuccessConfirm: onClose,
  });

  const initialPoolValue: PoolBody = {
    description: "",
    include_deferred: false,
    name: "",
    slots: 0,
    team_name: "",
  };

  const handleClose = () => {
    setError(undefined);
    onClose();
  };

  return (
    <>
      <Toaster />
      <Button onClick={onOpen}>
        <MdAdd /> {translate("pools.add")}
      </Button>

      <Modal onOpenChange={handleClose} open={open} title={translate("pools.add")}>
        <PoolForm
          error={error}
          initialPool={initialPoolValue}
          isPending={isPending}
          manageMutate={addPool}
          setError={setError}
        />
      </Modal>
    </>
  );
};

export default AddPoolButton;
