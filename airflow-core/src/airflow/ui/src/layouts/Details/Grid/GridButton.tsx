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
import { Flex, type FlexProps } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";

import type { DagRunState, TaskInstanceState } from "openapi/requests/types.gen";
import { BasicTooltip } from "src/components/BasicTooltip";

type Props = {
  readonly dagId: string;
  readonly isGroup?: boolean;
  readonly label: string;
  readonly runId: string;
  readonly searchParams: string;
  readonly state: DagRunState | TaskInstanceState | null | undefined;
  readonly taskId?: string;
} & FlexProps;

export const GridButton = ({
  children,
  dagId,
  isGroup,
  label,
  runId,
  searchParams,
  state,
  taskId,
  ...rest
}: Props) => {
  const { t: translate } = useTranslation();

  const tooltipContent = (
    <>
      {label}
      <br />
      {translate("state")}:{" "}
      {state ? translate(`common:states.${state}`) : translate("common:states.no_status")}
    </>
  );

  return isGroup ? (
    <BasicTooltip content={tooltipContent}>
      <Flex
        background={`${state}.solid`}
        borderRadius={2}
        height="10px"
        minW="14px"
        pb="2px"
        px="2px"
        {...rest}
      >
        {children}
      </Flex>
    </BasicTooltip>
  ) : (
    <BasicTooltip content={tooltipContent}>
      <Link
        replace
        to={{
          pathname: `/dags/${dagId}/runs/${runId}/${taskId === undefined ? "" : `tasks/${taskId}`}`,
          search: searchParams.toString(),
        }}
      >
        <Flex
          background={`${state}.solid`}
          borderRadius={2}
          height="10px"
          pb="2px"
          px="2px"
          width="14px"
          {...rest}
        >
          {children}
        </Flex>
      </Link>
    </BasicTooltip>
  );
};
