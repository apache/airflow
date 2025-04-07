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
import { Link } from "@chakra-ui/react";

import { urlRegex } from "src/constants/urlRegex";

export const LogLinks = ({ content }: { content: string }) => {
  const matches = [...content.matchAll(urlRegex)];
  const elements: Array<JSX.Element> = [];

  let currentIndex = 0;

  matches.forEach((match, idx) => {
    const startIndex = match.index;

    // Add text before the URL
    if (startIndex > currentIndex) {
      // eslint-disable-next-line react/no-array-index-key
      elements.push(<span key={`text-${idx}`}>{content.slice(currentIndex, startIndex)}</span>);
    }

    // Add the URL as a link
    const url =
      match[0].startsWith("http") || match[0].startsWith("ws") || match[0].startsWith("ftp")
        ? match[0]
        : `http://${match[0]}`;

    elements.push(
      <Link
        color="fg.info"
        href={url}
        // eslint-disable-next-line react/no-array-index-key
        key={`link-${idx}`}
        rel="noopener noreferrer"
        target="_blank"
        textDecoration="underline"
      >
        {match[0]}
      </Link>,
    );

    currentIndex = startIndex + match[0].length;
  });

  // Add remaining text after the last URL
  if (currentIndex < content.length) {
    elements.push(<span key="text-final">{content.slice(currentIndex)}</span>);
  }

  return elements;
};
