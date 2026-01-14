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
import { Button, type ButtonProps, IconButton } from "@chakra-ui/react";
import type { FC, ReactElement } from "react";

import { Tooltip } from "src/components/ui";

export type ActionButtonProps = {
  readonly actionName: string;
  readonly icon: ReactElement;
  readonly withText?: boolean;
} & ButtonProps;

const ActionButton = ({
  actionName,
  colorPalette = "brand",
  icon,
  variant = "ghost",
  withText = false,
  ...rest
}: ActionButtonProps) => {
  const ButtonComponent: FC<ButtonProps> = withText ? Button : IconButton;

  return (
    <Tooltip content={actionName} disabled={withText}>
      {/* Extra div required for the Tooltip to be properly positioned if the ActionButton is used inside a Menu component*/}
      <div>
        <ButtonComponent
          {...rest}
          aria-label={actionName}
          colorPalette={colorPalette}
          size="md"
          title={actionName}
          variant={variant}
        >
          {icon}
          {withText ? actionName : undefined}
        </ButtonComponent>
      </div>
    </Tooltip>
  );
};

export default ActionButton;
