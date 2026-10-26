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
import { Skeleton, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { LuFileWarning } from "react-icons/lu";

import { useDagWarningServiceListDagWarnings } from "openapi/queries";

import { DAGWarningsModal } from "src/components/DAGWarningsModal";
import type { ExpandedApiError } from "src/components/ErrorAlert";
import { StatsCard } from "src/components/StatsCard";

import { useAutoRefresh } from "src/utils";

const PARSE_FAILURE_WARNING_TYPE = "user:dynamic_dag_failure";

export const ParseFailureWarnings = () => {
  const { onClose, onOpen, open } = useDisclosure();
  const refetchInterval = useAutoRefresh({ checkPendingRuns: true });
  const { i18n, t: translate } = useTranslation(["dashboard"]);
  const { data, error, isLoading } = useDagWarningServiceListDagWarnings(
    { limit: 100, warningType: PARSE_FAILURE_WARNING_TYPE },
    undefined,
    { refetchInterval },
  );
  const count = data?.total_entries ?? 0;

  if (isLoading) {
    return <Skeleton borderRadius="lg" height="42px" width="175px" />;
  }
  if (Boolean(error) && (error as ExpandedApiError).status === 403) {
    return undefined;
  }
  if (count === 0) {
    return undefined;
  }

  return (
    <>
      <StatsCard
        colorScheme="failed"
        count={count}
        icon={<LuFileWarning />}
        isLoading={false}
        isRTL={i18n.dir() === "rtl"}
        label={translate("dashboard:stats.parseFailureWarnings", { count })}
        onClick={onOpen}
      />
      <DAGWarningsModal
        heading={translate("dashboard:stats.parseFailureWarnings", { count })}
        onClose={onClose}
        open={open}
        warnings={data?.dag_warnings}
      />
    </>
  );
};
