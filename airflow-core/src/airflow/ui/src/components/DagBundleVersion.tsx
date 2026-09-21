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
import { Link, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { Tooltip } from "src/system-components";

// A git bundle stores the full 40-char hexsha, so show the prefix an author recognises and keep
// the whole value on hover.
const SHORT_VERSION_LENGTH = 7;

type Props = {
  readonly bundleUrl: string | null;
  readonly lastRefreshed: string | null;
  readonly version: string | null;
};

/** The version a bundle currently holds, linked to the commit it names where that is known. */
export const DagBundleVersion = ({ bundleUrl, lastRefreshed, version }: Props) => {
  const { t: translate } = useTranslation("browse");

  if (version === null) {
    // A versioning-capable bundle also reports null until its first successful refresh, so an
    // absent last_refreshed is what separates "not refreshed yet" from "not versioned".
    return (
      <Text color="fg.muted">
        {lastRefreshed === null
          ? translate("dagBundles.notRefreshedYet")
          : translate("dagBundles.notVersioned")}
      </Text>
    );
  }

  const short = version.slice(0, SHORT_VERSION_LENGTH);

  return (
    <Tooltip content={version}>
      {bundleUrl === null ? (
        <Text fontFamily="mono">{short}</Text>
      ) : (
        <Link color="fg.info" fontFamily="mono" href={bundleUrl} rel="noreferrer" target="_blank">
          {short}
        </Link>
      )}
    </Tooltip>
  );
};
