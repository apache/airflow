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
import { Heading, Separator } from "@chakra-ui/react";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import { useConfigServiceGetConfig, useAuthLinksServiceGetAuthMenus } from "openapi/queries";
import { DataTable } from "src/components/DataTable";
import { ErrorAlert } from "src/components/ErrorAlert";

const createColumns = (translate: (key: string) => string) => [
  {
    accessorKey: "section",
    header: translate("columns.section"),
  },
  {
    accessorKey: "key",
    header: translate("columns.option"),
  },
  {
    accessorKey: "value",
    header: translate("columns.value"),
  },
];

export const Configs = () => {
  const { t: translate } = useTranslation(["admin", "common"]);
  const { data: authLinks, error: authError, isLoading: isAuthLoading } = useAuthLinksServiceGetAuthMenus();

  // All hooks must be called at the top level, before any returns
  const hasConfigAccess = authLinks?.authorized_menu_items.includes("Config") ?? false;
  const { data, error } = useConfigServiceGetConfig(undefined, undefined, {
    enabled: Boolean(authLinks) && hasConfigAccess,
  });
  const columns = useMemo(() => createColumns(translate), [translate]);

  // Handle auth loading state
  if (isAuthLoading) {
    return (
      <>
        <Heading mb={4}>{translate("config.title")}</Heading>
        <Separator />
        <div>{translate("common:loading")}</div>
      </>
    );
  }

  // Handle auth error state
  if (Boolean(authError)) {
    return (
      <>
        <Heading mb={4}>{translate("config.title")}</Heading>
        <Separator />
        <ErrorAlert error={authError} />
      </>
    );
  }

  const render =
    data?.sections.flatMap((section) =>
      section.options.map((option) => ({
        ...option,
        section: section.name,
      })),
    ) ?? [];

  // Show access denied message if user doesn't have config permissions
  if (!hasConfigAccess) {
    return (
      <>
        <Heading mb={4}>{translate("config.title")}</Heading>
        <Separator />
        <ErrorAlert error={{ message: translate("common:errors.accessDenied") }} />
      </>
    );
  }

  return (
    <>
      <Heading mb={4}>{translate("config.title")}</Heading>
      <Separator />
      {error === null ? (
        <DataTable columns={columns} data={render} modelName={translate("common:admin.Config")} />
      ) : (
        <ErrorAlert error={error} />
      )}
    </>
  );
};