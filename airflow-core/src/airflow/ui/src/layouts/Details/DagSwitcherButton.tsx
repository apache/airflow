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
import { SearchDags, useDagSearchOptions } from "src/components/SearchDags";

import { SHORTCUTS } from "src/context/keyboardShortcuts";
import { useShortcut } from "src/hooks/useShortcut";

type Props = {
  readonly children: ReactNode;
  readonly dagId: string;
  readonly shape: CrumbShape;
};

/** The Dag level of the breadcrumb, with the Dag search behind its chevron. */
export const DagSwitcherButton = ({ children, dagId, shape }: Props) => {
  const { t: translate } = useTranslation();
  const [open, setOpen] = useState(false);
  const dags = useDagSearchOptions();

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
