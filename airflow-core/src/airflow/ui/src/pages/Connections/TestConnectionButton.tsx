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
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiActivity, FiWifi, FiWifiOff } from "react-icons/fi";

import type { ConnectionResponse, ConnectionBody } from "openapi/requests/types.gen";
import ActionButton from "src/components/ui/ActionButton";
import { useConfig } from "src/queries/useConfig";
import { useTestConnection } from "src/queries/useTestConnection";
import { Tooltip } from "src/components/ui";

type TestConnectionOption = "Disabled" | "Enabled" | "Hidden";
type Props = {
  readonly connection: ConnectionResponse;
};

const defaultIcon = <FiActivity />;
const connectedIcon = <FiWifi color="green" />;
const disconnectedIcon = <FiWifiOff color="red" />;

const TestConnectionButton = ({ connection }: Props) => {
  const { t: translate } = useTranslation("admin");
  const [icon, setIcon] = useState(defaultIcon);
  const [message, setMessage] = useState<string | undefined>(undefined);
  const testConnection = useConfig("test_connection");
  let option: TestConnectionOption;

  if (testConnection === "Enabled") {
    option = "Enabled";
  } else if (testConnection === "Hidden") {
    option = "Hidden";
  } else {
    option = "Disabled";
  }

  const connectionBody: ConnectionBody = {
    conn_type: connection.conn_type,
    connection_id: connection.connection_id,
    description: connection.description ?? "",
    extra: connection.extra === "" || connection.extra === null ? "{}" : connection.extra,
    host: connection.host ?? "",
    login: connection.login ?? "",
    password: connection.password ?? "",
    port: Number(connection.port),
    schema: connection.schema ?? "",
  };

  const { isPending, mutate } = useTestConnection((result) => {
    if (result === undefined) {
      setIcon(defaultIcon);
      setMessage(undefined);
    } else if (result === true) {
      setIcon(connectedIcon);
      // Message will be set by the hook's onSuccess callback
    } else {
      setIcon(disconnectedIcon);
      // Message will be set by the hook's onSuccess callback
    }
  }, (newMessage) => {
    setMessage(newMessage);
  });

  const tooltipContent = message ? message : translate("connections.test");
  
  return (
    <div style={{ position: 'relative' }}>
      <Tooltip content={tooltipContent}>
        <ActionButton
          actionName={
            option === "Enabled" ? translate("connections.test") : translate("connections.testDisabled")
          }
          disabled={option === "Disabled"}
          display={option === "Hidden" ? "none" : "flex"}
          icon={icon}
          loading={isPending}
          onClick={() => {
            // Reset message when starting a new test
            setMessage(undefined);
            mutate({ requestBody: connectionBody });
          }}
          text={translate("connections.test")}
          withText={false}
        />
      </Tooltip>
      {message && (
        <div style={{
          position: 'absolute',
          top: '100%',
          left: '50%',
          transform: 'translateX(-50%)',
          background: 'rgba(0, 0, 0, 0.8)',
          color: 'white',
          padding: '4px 8px',
          borderRadius: '4px',
          fontSize: '12px',
          whiteSpace: 'nowrap',
          zIndex: 1000,
          marginTop: '4px',
        }}>
          {message}
        </div>
      )}
    </div>
  );
};

export default TestConnectionButton;
