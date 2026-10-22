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
import { Center, CloseButton, Drawer, Portal, Spinner, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { useWebhookServiceGetWebhookDeliveries } from "openapi/queries";

import { ErrorAlert } from "src/components/ErrorAlert";

import { DeliveryDetails } from "./DeliveryDetails";

type Props = {
  readonly deliveryId?: string;
  readonly onClose: () => void;
  readonly webhookName: string;
};

export const DeliveryDetailsDrawer = ({ deliveryId, onClose, webhookName }: Props) => {
  const { t: translate } = useTranslation(["browse", "common"]);

  // Fetched on its own, so a link to a delivery opens it whatever page of the table is shown.
  const { data, error, isLoading } = useWebhookServiceGetWebhookDeliveries(
    { id: deliveryId, limit: 1, webhookName },
    undefined,
    { enabled: deliveryId !== undefined },
  );
  const [delivery] = data?.deliveries ?? [];

  return (
    <Drawer.Root
      lazyMount
      onOpenChange={(event) => {
        if (!event.open) {
          onClose();
        }
      }}
      open={deliveryId !== undefined}
      size="lg"
      unmountOnExit
    >
      <Portal>
        <Drawer.Backdrop />
        <Drawer.Positioner>
          <Drawer.Content data-testid="delivery-details">
            <Drawer.Header>
              <Drawer.Title>{translate("webhookDeliveries.details.title")}</Drawer.Title>
            </Drawer.Header>
            <Drawer.CloseTrigger asChild>
              <CloseButton />
            </Drawer.CloseTrigger>
            <Drawer.Body>
              <ErrorAlert error={error} />
              {isLoading ? (
                <Center py={6}>
                  <Spinner
                    aria-label={translate("common:loading")}
                    data-testid="delivery-details-loading"
                    size="lg"
                  />
                </Center>
              ) : undefined}
              {delivery === undefined ? (
                isLoading || Boolean(error) ? undefined : (
                  <Text>{translate("webhookDeliveries.details.notFound")}</Text>
                )
              ) : (
                <DeliveryDetails delivery={delivery} />
              )}
            </Drawer.Body>
          </Drawer.Content>
        </Drawer.Positioner>
      </Portal>
    </Drawer.Root>
  );
};
