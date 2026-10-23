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
import { useState } from "react";

import { Stack, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiAlertTriangle } from "react-icons/fi";
import { useDebouncedCallback } from "use-debounce";

import { NumberInputField, NumberInputRoot, Tooltip } from "src/system-components";

// Mirrors MAX_ALLOWED_CHANGES on the diff endpoint, which is the real guard: this only spares
// the caller a rejected request, so a stale value costs a 422 the error alert explains.
export const MAX_CHANGES_LIMIT = 5000;
// Mirrors DEFAULT_MAX_CHANGES on the diff endpoint, so the truncation notice can name the limit
// that cut the comparison short when the caller left the field empty.
const DEFAULT_MAX_CHANGES = 500;
// Every distinct bound is its own request, and each one re-reads and compares two whole serialized
// Dags, so a keystroke is too cheap a trigger.
const MAX_CHANGES_DEBOUNCE_MS = 400;

type MaxChangesFieldProps = {
  /** The bound in force; undefined means the endpoint's own default applied. */
  readonly maxChanges?: number;
  readonly onCommit: (value: string) => void;
  /** Whether the comparison in view was cut short by that bound. */
  readonly truncated: boolean;
};

export const MaxChangesField = ({ maxChanges, onCommit, truncated }: MaxChangesFieldProps) => {
  const { t: translate } = useTranslation("dag");
  // The field keeps what was typed while the debounced write settles; the URL only ever holds a
  // bound the endpoint accepts.
  const [draft, setDraft] = useState(maxChanges === undefined ? "" : `${maxChanges}`);
  const write = useDebouncedCallback(onCommit, MAX_CHANGES_DEBOUNCE_MS);

  // Explicit because the number input otherwise flags an empty field as out of range (an empty
  // value parses to NaN), which drew an error border around the field's normal, unset state.
  const truncationTitle = translate("versions.limitReached.title", {
    limit: maxChanges ?? DEFAULT_MAX_CHANGES,
  });

  const draftNumber = Number(draft);
  const invalid =
    draft !== "" && (!Number.isInteger(draftNumber) || draftNumber < 1 || draftNumber > MAX_CHANGES_LIMIT);

  return (
    <VStack alignItems="flex-start" gap={1}>
      <Stack alignItems="center" direction="row" gap={1}>
        <Text fontSize="xs">{translate("versions.maxChanges")}</Text>
        {/* The bound that cut the comparison short is the one control that can lift it, so the
            notice sits on the field rather than as a banner over the table it describes. */}
        {truncated ? (
          <Tooltip
            content={
              <Stack gap={1}>
                <Text fontSize="sm" fontWeight="bold">
                  {truncationTitle}
                </Text>
                <Text fontSize="sm">
                  {translate(
                    maxChanges === undefined
                      ? "versions.limitReached.descriptionDefault"
                      : "versions.limitReached.description",
                  )}
                </Text>
              </Stack>
            }
            contentProps={{ maxW: "320px" }}
            portalled
          >
            <Text
              aria-label={truncationTitle}
              as="button"
              color="warning.fg"
              data-testid="max-changes-truncated"
              display="flex"
            >
              <FiAlertTriangle />
            </Text>
          </Tooltip>
        ) : undefined}
      </Stack>
      <NumberInputRoot
        invalid={invalid}
        max={MAX_CHANGES_LIMIT}
        min={1}
        onValueChange={({ value }) => {
          setDraft(value);
          write(value);
        }}
        size="sm"
        value={draft}
        w={28}
      >
        <NumberInputField
          aria-label={translate("versions.maxChanges")}
          placeholder={translate("versions.maxChangesDefault")}
        />
      </NumberInputRoot>
    </VStack>
  );
};
