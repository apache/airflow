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

import { Skeleton, Button, HStack, Heading, Text, Link, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiAlertCircle } from "react-icons/fi";
import { MdOutlineOpenInFull } from "react-icons/md";

import { useXcomServiceGetXcomEntry } from "openapi/queries";
import type { XComResponseNative } from "openapi/requests/types.gen";

import { ClipboardIconButton, ClipboardRoot, Dialog, IconButton, Modal } from "src/system-components";

import { ErrorAlert, type ExpandedApiError } from "src/components/ErrorAlert";
import { ErrorModal } from "src/components/ErrorModal";
import RenderedJsonField from "src/components/RenderedJsonField";

import { urlRegex } from "src/constants/urlRegex";

type XComEntryProps = {
  readonly dagId: string;
  readonly mapIndex: number;
  readonly open?: boolean;
  readonly regionId?: string;
  readonly regionIndex?: number;
  readonly runId: string;
  readonly taskId: string;
  readonly xcomKey: string;
};

const MAX_PREVIEW_LENGTH = 200;
const MAX_PREVIEW_LINES = 5;

const isLongText = (text: string) =>
  text.length > MAX_PREVIEW_LENGTH || text.split("\n").length > MAX_PREVIEW_LINES;

const renderTextWithLinks = (text: string) => {
  const urls = text.match(urlRegex);
  const parts = text.split(/(\s+)/u);

  return (
    <>
      {parts.map((part) => {
        if (urls?.includes(part)) {
          return (
            <Link
              color="fg.info"
              href={part}
              key={part}
              rel="noopener noreferrer"
              target="_blank"
              textDecoration="underline"
            >
              {part}
            </Link>
          );
        }

        return part;
      })}
    </>
  );
};

const XComError = ({ error }: { readonly error: unknown }) => {
  const { t: translate } = useTranslation("common");
  const [errorOpen, setErrorOpen] = useState(false);
  const { status } = error as Partial<ExpandedApiError>;
  const errorTitle = translate("error.title");

  return (
    <>
      <Button
        aria-haspopup="dialog"
        aria-label={status === undefined ? errorTitle : `${errorTitle} ${String(status)}`}
        color="fg.error"
        data-testid="xcom-entry-error"
        fontSize="sm"
        gap={1}
        onClick={() => setErrorOpen(true)}
        px={0}
        size="xs"
        variant="plain"
      >
        <FiAlertCircle aria-hidden="true" />
        {status ?? errorTitle}
      </Button>
      <ErrorModal
        icon={<FiAlertCircle />}
        onClose={() => setErrorOpen(false)}
        open={errorOpen}
        title={errorTitle}
      >
        <ErrorAlert error={error} />
      </ErrorModal>
    </>
  );
};

const XComFullValue = ({ value }: { readonly value: string }) => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const { onClose, onOpen, open } = useDisclosure();

  return (
    <>
      <IconButton
        aria-haspopup="dialog"
        aria-label={translate("common:expand.expand")}
        data-testid="xcom-view-full"
        label={translate("common:expand.expand")}
        onClick={onOpen}
      >
        <MdOutlineOpenInFull />
      </IconButton>
      <Modal
        bodyProps={{ overflowWrap: "anywhere" }}
        contentProps={{ padding: 4 }}
        headerProps={{
          children: (
            <HStack gap={2} minW={0}>
              <Dialog.Title asChild>
                <Heading fontSize="lg">{translate("xcom.value")}</Heading>
              </Dialog.Title>
              <ClipboardRoot value={value}>
                <ClipboardIconButton />
              </ClipboardRoot>
            </HStack>
          ),
        }}
        lazyMount
        onOpenChange={onClose}
        open={open}
        scrollBehavior="inside"
        size="full"
        unmountOnExit
      >
        <Text data-testid="xcom-full-value" overflowWrap="anywhere" whiteSpace="pre-wrap">
          {renderTextWithLinks(value)}
        </Text>
      </Modal>
    </>
  );
};

export const XComEntry = ({
  dagId,
  mapIndex,
  open = false,
  regionId,
  regionIndex,
  runId,
  taskId,
  xcomKey,
}: XComEntryProps) => {
  const { data, error, isLoading } = useXcomServiceGetXcomEntry<XComResponseNative>({
    dagId,
    dagRunId: runId,
    deserialize: true,
    mapIndex,
    regionId,
    regionIndex,
    stringify: false,
    taskId,
    xcomKey,
  });

  if (Boolean(error)) {
    return <XComError error={error} />;
  }

  // When deserialize=true, the API returns a stringified representation
  // so we don't need to JSON.stringify it again
  const xcomValue = data?.value;
  const isObjectOrArray = Array.isArray(xcomValue) || (xcomValue !== null && typeof xcomValue === "object");
  const valueFormatted = typeof xcomValue === "string" ? xcomValue : JSON.stringify(xcomValue, undefined, 4);
  const hasNewline = typeof xcomValue === "string" && xcomValue.includes("\n");
  const showViewFull = typeof xcomValue === "string" && isLongText(xcomValue);

  return isLoading ? (
    <Skeleton
      data-testid="skeleton"
      display="inline-block"
      height="10px"
      width={200} // TODO: Make Skeleton take style from column definition
    />
  ) : (
    <HStack data-testid="xcom-value">
      {isObjectOrArray ? (
        <RenderedJsonField collapsed={!open} content={xcomValue as object} enableClipboard={false} />
      ) : (
        <Text
          maxHeight="200px"
          minWidth={hasNewline ? "250px" : undefined}
          overflowWrap="anywhere"
          overflowY="auto"
          whiteSpace="pre-wrap"
        >
          {renderTextWithLinks(valueFormatted)}
        </Text>
      )}
      {showViewFull ? <XComFullValue value={valueFormatted} /> : undefined}
      {xcomValue === undefined || xcomValue === null ? undefined : (
        <ClipboardRoot value={valueFormatted}>
          <ClipboardIconButton />
        </ClipboardRoot>
      )}
    </HStack>
  );
};
