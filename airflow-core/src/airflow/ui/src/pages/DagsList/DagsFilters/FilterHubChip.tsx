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
import { Button, ButtonGroup, CloseButton, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

export type FilterHubFacet =
  | "activeRunState"
  | "favorite"
  | "lastRunState"
  | "needsReview"
  | "owners"
  | "paused"
  | "tags"
  | "teams"
  | "timetableTypes";

export type FilterHubChipData = {
  readonly accessibleSummary: string;
  readonly facet: FilterHubFacet;
  readonly label: string;
  readonly onRemove: () => void;
  readonly summary: string;
};

type FilterHubChipProps = {
  readonly chip: FilterHubChipData;
  readonly editButtonRef: (node: HTMLButtonElement | null) => void;
  readonly onEdit: () => void;
  readonly onRemove: () => void;
};

export const FilterHubChip = ({ chip, editButtonRef, onEdit, onRemove }: FilterHubChipProps) => {
  const { t: translate } = useTranslation("dags");
  const name = `${chip.label}: ${chip.accessibleSummary}`;

  return (
    <ButtonGroup attached size="sm">
      <Button
        aria-label={translate("filters.editFilter", { filter: name })}
        bg="bg.muted"
        data-testid={`hub-edit-${chip.facet}`}
        maxWidth={{ base: "240px", md: "360px" }}
        onClick={onEdit}
        ref={editButtonRef}
        variant="outline"
      >
        <Text as="span" color="fg.muted" flexShrink={0}>
          {chip.label}:
        </Text>
        <Text as="span" overflow="hidden" textOverflow="ellipsis" whiteSpace="nowrap">
          {chip.summary}
        </Text>
      </Button>
      <CloseButton
        aria-label={translate("filters.removeFilter", { filter: name })}
        bg="bg.muted"
        borderWidth="1px"
        data-testid={`hub-remove-${chip.facet}`}
        onClick={onRemove}
      />
    </ButtonGroup>
  );
};
