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

import {
  Box,
  Code,
  Divider,
  Heading,
  Image,
  ImageProps,
  Link,
  ListItem,
  OrderedList,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  UnorderedList,
} from "@chakra-ui/react";
import React, { PropsWithChildren, ReactNode } from "react";

import type { Components } from "react-markdown";

import ReactMD from "react-markdown";
import type { ReactMarkdownOptions } from "react-markdown/lib/react-markdown";

import remarkGfm from "remark-gfm";

const fontSizeMapping = {
  h1: "1.5em",
  h2: "1.25em",
  h3: "1.125em",
  h4: "1em",
  h5: "0.875em",
  h6: "0.75em",
};

const makeHeading =
  (h: keyof typeof fontSizeMapping) =>
  ({ children, ...props }: PropsWithChildren) =>
    (
      <Heading as={h} fontSize={fontSizeMapping[h]} {...props} my={3}>
        {children}
      </Heading>
    );

const components = {
  p: ({ children }: PropsWithChildren) => <Text>{children}</Text>,
  em: ({ children }: PropsWithChildren) => <Text as="em">{children}</Text>,
  blockquote: ({ children }: PropsWithChildren) => (
    <Box
      as="blockquote"
      borderLeft="solid 2px"
      borderColor="gray.400"
      fontStyle="italic"
      my={3}
      pl={2}
    >
      {children}
    </Box>
  ),
  code: ({
    inline,
    className,
    children,
  }: {
    inline?: boolean;
    className?: string;
    children: ReactNode;
  }) => {
    if (inline) {
      return <Code p={2}>{children}</Code>;
    }

    return (
      <Code
        whiteSpace="break-spaces"
        display="block"
        w="full"
        p={2}
        className={className}
      >
        {children}
      </Code>
    );
  },
  del: ({ children }: PropsWithChildren) => <Text as="del">{children}</Text>,
  hr: () => <Divider my={3} />,
  a: ({
    href,
    title,
    children,
  }: {
    href: string;
    title?: string;
    children: ReactNode;
  }) => (
    <Link fontWeight="bold" color="blue.600" href={href} title={title}>
      {children}
    </Link>
  ),
  img: (props: ImageProps) => <Image my={3} {...props} maxWidth="300px" />,
  text: ({ children }: PropsWithChildren) => <Text as="span">{children}</Text>,
  ul: ({ children }: PropsWithChildren) => (
    <UnorderedList spacing={1} pl={4} mb={3}>
      {children}
    </UnorderedList>
  ),
  ol: ({ children }: PropsWithChildren) => (
    <OrderedList spacing={1} pl={4} mb={3}>
      {children}
    </OrderedList>
  ),
  li: ({ children }: PropsWithChildren) => <ListItem>{children}</ListItem>,
  h1: makeHeading("h1"),
  h2: makeHeading("h2"),
  h3: makeHeading("h3"),
  h4: makeHeading("h4"),
  h5: makeHeading("h5"),
  h6: makeHeading("h6"),
  pre: ({ children }: PropsWithChildren) => <Code my={3}>{children}</Code>,
  table: ({ children }: PropsWithChildren) => <Table mb={3}>{children}</Table>,
  thead: Thead,
  tbody: Tbody,
  tr: Tr,
  td: Td,
  th: Th,
};

const ReactMarkdown = (props: ReactMarkdownOptions) => (
  <ReactMD
    components={components as Components}
    {...props}
    skipHtml
    remarkPlugins={[remarkGfm]}
  />
);

export default ReactMarkdown;
