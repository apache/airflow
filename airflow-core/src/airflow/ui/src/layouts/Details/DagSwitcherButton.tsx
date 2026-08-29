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
import { type ReactNode, useState } from "react";
import { useTranslation } from "react-i18next";
import { BsChevronExpand } from "react-icons/bs";

import {
  CUT_PADDING,
  CrumbDivider,
  CrumbGroup,
  CrumbLink,
  type CrumbShape,
  crumbButtonStyles,
  getWedgePadding,
} from "src/components/Breadcrumb";
import { SearchDags } from "src/components/SearchDags";
import { IconButton, Popover, Tooltip } from "src/components/ui";
import { SHORTCUTS } from "src/context/keyboardShortcuts";
import { useShortcut } from "src/hooks/useShortcut";

type Props = {
  readonly children: ReactNode;
  readonly dagId: string;
  readonly shape: CrumbShape;
};

/**
 * The Dag level of the breadcrumb: a two-part control whose left half navigates to the Dag and
 * whose right half opens the Dag search, so switching Dags happens where the current one is named.
 */
export const DagSwitcherButton = ({ children, dagId, shape }: Props) => {
  const { t: translate } = useTranslation();
  const [open, setOpen] = useState(false);

  useShortcut({
    ...SHORTCUTS.search.searchDags,
    callback: () => setOpen(true),
    dependencies: [open],
    options: { preventDefault: true },
  });

  return (
    <Popover.Root
      lazyMount
      onOpenChange={(event) => setOpen(event.open)}
      open={open}
      positioning={{ placement: "bottom-start" }}
      unmountOnExit
    >
      {/* Anchored to the whole Dag level rather than the chevron: it shares the breadcrumb's start
          edge and its bottom, so the panel drops clear of the bar and lines up with it. */}
      <Popover.Anchor asChild>
        <CrumbGroup shape={shape}>
          <CrumbLink paddingInlineEnd={CUT_PADDING} to={`/dags/${dagId}`}>
            {children}
          </CrumbLink>
          <CrumbDivider />
          {/* The tooltip wraps the trigger rather than coming from IconButton's `label`: nesting it
            inside `asChild` leaves its own trigger ref unset, and it renders away from the button. */}
          <Tooltip content={translate("switchDag")} disabled={open} portalled>
            <Popover.Trigger asChild>
              <IconButton
                {...crumbButtonStyles}
                {...getWedgePadding(shape)}
                alignSelf="stretch"
                aria-label={translate("switchDag")}
                data-testid="switch-dag"
                paddingInlineStart={2}
              >
                <BsChevronExpand />
              </IconButton>
            </Popover.Trigger>
          </Tooltip>
        </CrumbGroup>
      </Popover.Anchor>
      <Popover.Content data-testid="switch-dag-popover" width="sm">
        <Box p={2}>
          <SearchDags onClose={() => setOpen(false)} />
        </Box>
      </Popover.Content>
    </Popover.Root>
  );
};
