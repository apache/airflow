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
import { Heading, Text, HStack } from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { LuSettings } from "react-icons/lu";

import { useConfigServiceGetBackendsOrderValue } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import { Dialog } from "src/components/ui";

type BackendsOrderModalProps = {
  onClose: () => void;
  open: boolean;
};

export const BackendsOrderModal: React.FC<BackendsOrderModalProps> = ({ onClose, open }) => {
  const { t: translate } = useTranslation("admin");
  const [backendsOrder, setBackendsOrder] = useState<Array<string> | string>();
  const { data, error } = useConfigServiceGetBackendsOrderValue();

  const onOpenChange = () => {
    onClose();
  };

  useEffect(() => {
    setBackendsOrder(data?.sections[0]?.options[0]?.value ?? "");
  }, [data, open]);

  return (
    <Dialog.Root onOpenChange={onOpenChange} open={open} scrollBehavior="inside" size="md">
      <Dialog.Content backdrop p={4}>
        <Dialog.Header display="flex" justifyContent="space-between">
          <HStack fontSize="xl">
            <LuSettings />
            <Heading size="md">{translate("variables.backendsOrder")}</Heading>
          </HStack>
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body>
          {Boolean(error) ? <ErrorAlert error={error} /> : null}
          <Text fontFamily="mono" fontSize="sm" whiteSpace="pre-wrap">
            {backendsOrder}
          </Text>
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};
