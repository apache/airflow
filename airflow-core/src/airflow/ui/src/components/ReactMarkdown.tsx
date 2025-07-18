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
  Separator,
  Heading,
  Image,
  type ImageProps,
  Link,
  List,
  Table,
  Text,
} from "@chakra-ui/react";
import type { PropsWithChildren, ReactNode } from "react";
import type { Components, Options } from "react-markdown";
import ReactMD from "react-markdown";
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
  (header: keyof typeof fontSizeMapping) =>
  ({ children, ...props }: PropsWithChildren) => (
    <Heading as={header} fontSize={fontSizeMapping[header]} {...props} my={3}>
      {children}
    </Heading>
  );

const components = {
  // eslint-disable-next-line id-length
  a: ({ children, href, title }: { children: ReactNode; href: string; title?: string }) => (
    <Link color="blue.600" fontWeight="bold" href={href} title={title}>
      {children}
    </Link>
  ),
  blockquote: ({ children }: PropsWithChildren) => (
    <Box as="blockquote" borderColor="gray.400" borderLeft="solid 2px" fontStyle="italic" my={3} pl={2}>
      {children}
    </Box>
  ),
  code: ({ children, className, inline }: { children: ReactNode; className?: string; inline?: boolean }) => {
    if (inline) {
      return (
        <Code display="inline" p={2}>
          {children}
        </Code>
      );
    }

    return (
      <Code className={className} display="block" p={2} w="full" whiteSpace="break-spaces">
        {children}
      </Code>
    );
  },
  del: ({ children }: PropsWithChildren) => <Text as="del">{children}</Text>,
  em: ({ children }: PropsWithChildren) => <Text as="em">{children}</Text>,
  h1: makeHeading("h1"),
  h2: makeHeading("h2"),
  h3: makeHeading("h3"),
  h4: makeHeading("h4"),
  h5: makeHeading("h5"),
  h6: makeHeading("h6"),
  hr: () => <Separator my={3} />,
  img: (props: ImageProps) => <Image my={3} {...props} maxWidth="300px" />,
  li: ({ children }: PropsWithChildren) => <List.Item>{children}</List.Item>,
  ol: ({ children }: PropsWithChildren) => (
    <List.Root as="ol" mb={3} pl={4}>
      {children}
    </List.Root>
  ),
  // eslint-disable-next-line id-length
  p: ({ children }: PropsWithChildren) => <Text>{children}</Text>,
  pre: ({ children }: PropsWithChildren) => <Code my={3}>{children}</Code>,
  table: ({ children }: PropsWithChildren) => <Table.Root mb={3}>{children}</Table.Root>,
  tbody: Table.Body,
  td: Table.Cell,
  text: ({ children }: PropsWithChildren) => <Text as="span">{children}</Text>,
  th: Table.ColumnHeader,
  thead: Table.Header,
  tr: Table.Row,
  ul: ({ children }: PropsWithChildren) => (
    <List.Root mb={3} pl={4}>
      {children}
    </List.Root>
  ),
};

const ReactMarkdown = (props: Options) => (
  <ReactMD components={components as Components} {...props} remarkPlugins={[remarkGfm]} skipHtml />
);

export default ReactMarkdown;
