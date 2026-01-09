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
import { Button, ButtonGroup } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

type Props = {
  readonly defaultShowPaused: string;
  readonly onPausedChange: React.MouseEventHandler<HTMLButtonElement>;
  readonly showPaused: string | null;
};

export const PausedFilter = ({ defaultShowPaused, onPausedChange, showPaused }: Props) => {
  const { t: translate } = useTranslation("dags");

  const currentValue = showPaused ?? defaultShowPaused;

  return (
    <ButtonGroup attached size="sm" variant="outline">
      <Button
        bg={currentValue === "all" ? "colorPalette.muted" : undefined}
        colorPalette="brand"
        onClick={onPausedChange}
        value="all"
        variant={currentValue === "all" ? "solid" : "outline"}
      >
        {translate("filters.paused.all")}
      </Button>
      <Button
        bg={currentValue === "false" ? "colorPalette.muted" : undefined}
        colorPalette="brand"
        onClick={onPausedChange}
        value="false"
        variant={currentValue === "false" ? "solid" : "outline"}
      >
        {translate("filters.paused.active")}
      </Button>
      <Button
        bg={currentValue === "true" ? "colorPalette.muted" : undefined}
        colorPalette="brand"
        onClick={onPausedChange}
        value="true"
        variant={currentValue === "true" ? "solid" : "outline"}
      >
        {translate("filters.paused.paused")}
      </Button>
    </ButtonGroup>
  );
};
