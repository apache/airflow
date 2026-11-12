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
import { Box } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiLink, FiZap } from "react-icons/fi";
import { MdDetails } from "react-icons/md";
import { Outlet, useParams } from "react-router-dom";

import { useTriggerServiceGetTrigger } from "openapi/queries";

import { Alert, ProgressBar, RouterLink } from "src/system-components";

import { NavTabs } from "src/layouts/Details/NavTabs";

import { TriggerUsedBy } from "src/pages/Triggers/TriggerUsedBy";
import { TriggersForbidden } from "src/pages/Triggers/TriggersForbidden";
import { getTriggerClassName } from "src/pages/Triggers/getTriggerClassName";

import { BreadcrumbRow, CrumbLink, CrumbStack, CrumbText } from "src/components/Breadcrumb";
import { ErrorAlert } from "src/components/ErrorAlert";
import { HeaderCard } from "src/components/HeaderCard";
import { TriggerStatusBadge } from "src/components/TriggerStatusBadge";

import { getErrorStatus, useAutoRefresh, useDocumentTitle } from "src/utils";

export const Trigger = () => {
  const { t: translate } = useTranslation(["browse", "common", "dag"]);
  const { triggerId = "" } = useParams();

  const refetchInterval = useAutoRefresh({});

  // Kept current for the tabs too, which read the same query: a trigger's status and failures change
  // on their own. Polling stops on an error, which is a 404 once the trigger has fired or is no longer used.
  const {
    data: trigger,
    error,
    isLoading,
  } = useTriggerServiceGetTrigger({ triggerId: Number(triggerId) }, undefined, {
    refetchInterval: ({ state }) => state.status !== "error" && refetchInterval,
  });

  useDocumentTitle(`${translate("common:trigger_one")} ${triggerId}`);

  const tabs = [
    { icon: <MdDetails />, label: translate("dag:tabs.details"), value: "" },
    { icon: <FiLink />, label: translate("triggers.columns.usedBy"), value: "used_by" },
  ];

  return (
    <Box p={2}>
      <BreadcrumbRow aria-label={translate("common:breadcrumb")} data-testid="trigger-breadcrumb" mb={3}>
        <CrumbLink shape={{ hasNotch: false, hasPoint: true }} to="/triggers">
          <CrumbStack caption={translate("common:nav.browse")} value={translate("common:browse.triggers")} />
        </CrumbLink>
        <CrumbText shape={{ hasNotch: true, hasPoint: false }}>
          <CrumbStack caption={translate("common:trigger_one")} isCurrent value={triggerId} />
        </CrumbText>
      </BreadcrumbRow>
      {/* A trigger's row is deleted once it fires or is no longer used, so a 404 is
          usually a trigger that has finished, possibly while this page was open. */}
      {getErrorStatus(error) === 404 ? (
        <Alert mb={2} status="info" title={translate("triggers.notFound.title")}>
          {translate("triggers.notFound.description")}{" "}
          <RouterLink to="/triggers">{translate("triggers.notFound.backToList")}</RouterLink>
        </Alert>
      ) : getErrorStatus(error) === 403 ? (
        <TriggersForbidden />
      ) : (
        <ErrorAlert error={error} />
      )}
      <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
      {trigger === undefined ? undefined : (
        <>
          <HeaderCard
            icon={<FiZap />}
            stats={[
              { label: translate("common:taskInstance.triggerer.id"), value: trigger.id },
              {
                label: translate("triggers.columns.kind"),
                value: translate(`triggers.kind.${trigger.kind}`),
              },
              { label: translate("triggers.columns.usedBy"), value: <TriggerUsedBy trigger={trigger} /> },
              { label: translate("triggers.columns.triggerer"), value: trigger.triggerer_hostname },
              { label: translate("triggers.columns.failures"), value: trigger.consecutive_failures },
            ]}
            statusBadge={<TriggerStatusBadge status={trigger.status} />}
            title={getTriggerClassName(trigger.classpath)}
            type="trigger"
          />
          <NavTabs tabs={tabs} />
          <Outlet />
        </>
      )}
    </Box>
  );
};
