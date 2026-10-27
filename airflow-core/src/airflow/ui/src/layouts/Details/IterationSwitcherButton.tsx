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

import { Button, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useLocation, useSearchParams } from "react-router-dom";

import { CrumbSwitcher, type CrumbShape } from "src/components/Breadcrumb";
import { LoopOptionLabel } from "src/components/LoopOptionLabel";

import { SearchParamsKeys } from "src/constants/searchParams";
import type { LoopOption } from "src/queries/useLoopFilterOptions";

type Props = {
  readonly children: ReactNode;
  /** Offer the whole loop as a choice. A group can show every pass at once; one row cannot. */
  readonly includeWholeLoop?: boolean;
  readonly options: Array<LoopOption>;
  readonly shape: CrumbShape;
};

/**
 * The loop iteration level of the breadcrumb, with the loop's passes behind its chevron.
 *
 * A pass is part of where you are rather than a filter you applied -- the same task id means a
 * different run of it in every pass -- so it switches where the current one is named. Picking one
 * rewrites the query rather than the path, so the level links to itself.
 */
export const IterationSwitcherButton = ({ children, includeWholeLoop = false, options, shape }: Props) => {
  const { t: translate } = useTranslation("common");
  const [open, setOpen] = useState(false);
  const [searchParams, setSearchParams] = useSearchParams();
  const { pathname } = useLocation();

  // A task group can be looked at whole, so it keeps the loop-wide entry; a task instance is
  // one row and must name a pass. Passes arrive newest first, so the newest leads.
  const passes = includeWholeLoop ? options : options.filter((option) => option.iteration !== undefined);
  // A table page carries the pass as a filter, a task instance page as a region coordinate.
  // Either names the same pass, so whichever is present decides what the level is showing.
  // A pass belongs to the loop that counted it, so one carried in from a different loop is
  // dropped rather than read as a position in this one.
  const carriedLoop = searchParams.get(SearchParamsKeys.LOOP_ID);
  const sameLoop = carriedLoop === null || carriedLoop === "" || carriedLoop === passes[0]?.loopId;
  const active = sameLoop
    ? (searchParams.get(SearchParamsKeys.ITERATION) ?? searchParams.get(SearchParamsKeys.REGION_INDEX))
    : null;
  const isActive = (option: LoopOption) =>
    active === null || active === "" ? option === passes[0] : String(option.iteration) === active;

  const choose = (option: LoopOption) => {
    const next = new URLSearchParams(searchParams);

    next.set(SearchParamsKeys.LOOP_ID, option.loopId);
    if (option.iteration === undefined) {
      next.delete(SearchParamsKeys.ITERATION);
    } else {
      next.set(SearchParamsKeys.ITERATION, String(option.iteration));
    }
    // The pass is named, never addressed. A coordinate points at one row, and this control does
    // not know which row a pass became: a mapped expansion lives in its own region, and a clear
    // forks later passes into a successor one. Dropping the old coordinate lets the page resolve
    // the pass against the server instead of being sent to a row that may not exist.
    next.delete(SearchParamsKeys.REGION_ID);
    next.delete(SearchParamsKeys.REGION_INDEX);
    // A page of one pass says nothing about where the next one starts, so paging restarts
    // rather than carrying a cursor into a different set of rows.
    next.delete(SearchParamsKeys.CURSOR);
    next.delete(SearchParamsKeys.OFFSET);
    setSearchParams(next);
    setOpen(false);
  };

  return (
    <CrumbSwitcher
      // A pass is a number and a badge, so the panel shrinks to them instead of taking the
      // width a search field needs.
      contentWidth="auto"
      label={translate("common:filters.switchIteration")}
      onOpenChange={setOpen}
      open={open}
      search={
        <VStack align="stretch" gap={0} maxHeight="320px" minWidth="32" overflowY="auto">
          {passes.map((option) => (
            <Button
              aria-current={isActive(option)}
              justifyContent="flex-start"
              key={option.value}
              onClick={() => choose(option)}
              size="sm"
              variant={isActive(option) ? "subtle" : "ghost"}
            >
              <LoopOptionLabel hideLoopName option={option} />
            </Button>
          ))}
        </VStack>
      }
      shape={shape}
      testId="switch-iteration"
      to={`${pathname}?${searchParams.toString()}`}
    >
      {children}
    </CrumbSwitcher>
  );
};
