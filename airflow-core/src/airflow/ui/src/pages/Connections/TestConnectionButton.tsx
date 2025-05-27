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
import { FiActivity, FiWifi, FiWifiOff } from "react-icons/fi";

import type { ConnectionResponse, ConnectionBody } from "openapi/requests/types.gen";
import ActionButton from "src/components/ui/ActionButton";
import { useConfig } from "src/queries/useConfig";
import { useTestConnection } from "src/queries/useTestConnection";

enum TestConnectionOption {
  Disabled = "Disabled",
  Enabled = "Enabled",
  Hidden = "Hidden",
}

type Props = {
  readonly connection: ConnectionResponse;
};

const TestConnectionButton = ({ connection }: Props) => {
  const defaultIcon = <FiActivity />;
  const connectedIcon = <FiWifi color="green" />;
  const disconnectedIcon = <FiWifiOff color="red" />;
  const [connected, setConnected] = useState<boolean | undefined>(undefined);
  const [icon, setIcon] = useState(defaultIcon);
  const [hasClicked, setHasClicked] = useState(false);

  const testConnection = useConfig("test_connection");
  let option;

  if (testConnection === "Enabled") {
    option = TestConnectionOption.Enabled;
  } else if (testConnection === "Hidden") {
    option = TestConnectionOption.Hidden;
  } else {
    option = TestConnectionOption.Disabled;
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

  const { mutate } = useTestConnection((result) => {
    setConnected(result);
    setHasClicked(true);
    if (result === undefined) {
      setIcon(defaultIcon);
    } else if (result === true) {
      setIcon(connectedIcon);
    } else {
      setIcon(disconnectedIcon);
    }
  });

  return (
    <ActionButton
      actionName={
        option === TestConnectionOption.Enabled
          ? "Test Connection"
          : "Testing connections disabled. Contact your admin to enable it."
      }
      disabled={option === TestConnectionOption.Disabled}
      display={option === TestConnectionOption.Hidden ? "none" : "flex"}
      icon={icon}
      onClick={() => {
        setHasClicked(false);
        mutate({ requestBody: connectionBody });
      }}
      onLoad={() => {
        setConnected(undefined);
        setHasClicked(false);
        setIcon(defaultIcon);
      }}
      onMouseLeave={() => {
        setHasClicked(false);
        if (connected === undefined) {
          setIcon(defaultIcon);
        } else if (connected) {
          setIcon(connectedIcon);
        } else {
          setIcon(disconnectedIcon);
        }
      }}
      onMouseMove={() => {
        if (!hasClicked) {
          setIcon(defaultIcon);
        }
      }}
      text="Test Connection"
      withText={false}
    />
  );
};

export default TestConnectionButton;
