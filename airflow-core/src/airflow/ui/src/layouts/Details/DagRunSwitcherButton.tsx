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

import { CrumbSwitcher, type CrumbShape } from "src/components/Breadcrumb";
import { SearchDagRuns } from "src/components/SearchDagRuns";

type Props = {
  readonly children: ReactNode;
  readonly dagId: string;
  readonly shape: CrumbShape;
  readonly to: string;
};

/**
 * The Dag run level of the breadcrumb, with a search over the Dag's runs behind its chevron. It
 * stands in for both a named run and the "all runs" level, so `to` comes from the crumb itself.
 */
export const DagRunSwitcherButton = ({ children, dagId, shape, to }: Props) => {
  const { t: translate } = useTranslation();
  const [open, setOpen] = useState(false);

  return (
    <CrumbSwitcher
      label={translate("switchDagRun")}
      onOpenChange={setOpen}
      open={open}
      search={<SearchDagRuns dagId={dagId} onClose={() => setOpen(false)} />}
      shape={shape}
      testId="switch-dag-run"
      to={to}
    >
      {children}
    </CrumbSwitcher>
  );
};
