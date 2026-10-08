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
import { type ReactNode, useState } from "react";

import { useTranslation } from "react-i18next";

import { useDagServiceGetDagsUi } from "openapi/queries";

import { CrumbSwitcher, type CrumbShape } from "src/components/Breadcrumb";

import { SHORTCUTS } from "src/context/keyboardShortcuts";
import { useShortcut } from "src/hooks/useShortcut";
import type { DagSearchOption } from "src/utils/option";

import { DAG_SEARCH_LIMIT, SearchDags, buildDagOption } from "./SearchDags";

const NO_DAGS: Array<DagSearchOption> = [];

type Props = {
  readonly children: ReactNode;
  readonly dagId: string;
  readonly shape: CrumbShape;
};

/**
 * The Dag level of the breadcrumb, with the Dag search behind its chevron.
 *
 * The Dags the panel lists are loaded here rather than inside it: a query that only starts when
 * the panel opens has nothing to show until it answers.
 */
export const DagSwitcherButton = ({ children, dagId, shape }: Props) => {
  const { t: translate } = useTranslation();
  const [open, setOpen] = useState(false);
  const { data } = useDagServiceGetDagsUi({ dagRunsLimit: 1, limit: DAG_SEARCH_LIMIT });
  const dags = data === undefined ? NO_DAGS : data.dags.map(buildDagOption);

  useShortcut({
    ...SHORTCUTS.search.searchDags,
    callback: () => setOpen(true),
    dependencies: [open],
    options: { preventDefault: true },
  });

  return (
    <CrumbSwitcher
      label={translate("switchDag")}
      onOpenChange={setOpen}
      open={open}
      search={<SearchDags dags={dags} onClose={() => setOpen(false)} />}
      shape={shape}
      testId="switch-dag"
      to={`/dags/${dagId}`}
    >
      {children}
    </CrumbSwitcher>
  );
};
