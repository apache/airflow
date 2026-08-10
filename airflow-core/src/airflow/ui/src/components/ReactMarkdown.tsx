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
import { Children, isValidElement } from "react";
import type { ComponentProps, PropsWithChildren, ReactNode } from "react";
import ReactMD from "react-markdown";
import type { Components, Options } from "react-markdown";
import remarkGfm from "remark-gfm";

import { useColorMode } from "src/context/colorMode";
import { oneDark, oneLight, type SyntaxTheme } from "src/utils/syntaxHighlighter";

import { MarkdownCodeBlock } from "./ReactMarkdownBlocks";

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

// Static components that don't depend on props

type MarkdownLinkProps = {
  readonly children: ReactNode;
  readonly href: string;
  readonly title?: string;
};

const MarkdownLink = ({ children, href, title }: MarkdownLinkProps) => (
  <Link color="fg.info" fontWeight="bold" href={href} rel="noopener noreferrer" target="_blank" title={title}>
    {children}
  </Link>
);

const LinkComponent = ({ children, href, title }: ComponentProps<"a">) => {
  if (href === undefined || children === undefined) {
    return children;
  }

  return (
    <MarkdownLink href={href} title={title}>
      {children}
    </MarkdownLink>
  );
};

const BlockquoteComponent = ({ children }: PropsWithChildren) => (
  <Box
    as="blockquote"
    borderColor="border.emphasized"
    borderLeft="solid 2px"
    fontStyle="italic"
    my={3}
    pl={2}
  >
    {children}
  </Box>
);

const DelComponent = ({ children }: PropsWithChildren) => <Text as="del">{children}</Text>;
const EmComponent = ({ children }: PropsWithChildren) => <Text as="em">{children}</Text>;
const HrComponent = () => <Separator my={3} />;
const ImgComponent = (imgProps: ImageProps) => <Image my={3} {...imgProps} maxWidth="300px" />;
const LiComponent = ({ children }: PropsWithChildren) => <List.Item>{children}</List.Item>;
const OlComponent = ({ children }: PropsWithChildren) => (
  <List.Root as="ol" mb={3} pl={4}>
    {children}
  </List.Root>
);

const markdownContentStyles = {
  "& .katex-display": {
    marginBlock: "0.75rem",
    overflowX: "auto",
    overflowY: "hidden",
  },
  "& .katex-display > .katex": {
    marginInline: "auto",
    width: "max-content",
  },
};

const PComponent = ({ children }: PropsWithChildren) => (
  <Text overflowWrap="break-word" wordBreak="break-word">
    {children}
  </Text>
);
const TableComponent = ({ children }: PropsWithChildren) => <Table.Root mb={3}>{children}</Table.Root>;
const TextComponent = ({ children }: PropsWithChildren) => <Text as="span">{children}</Text>;
const UlComponent = ({ children }: PropsWithChildren) => (
  <List.Root mb={3} pl={4}>
    {children}
  </List.Root>
);

type MarkdownCodeElementProps = {
  readonly children?: ReactNode;
  readonly className?: string;
};

const InlineCodeComponent = ({ children }: PropsWithChildren) => <Code display="inline">{children}</Code>;

// Factory function for the pre component that needs style
const createPreComponent =
  (style: SyntaxTheme) =>
  ({ children }: { readonly children?: ReactNode }) => {
    const [codeElement] = Children.toArray(children);

    if (!isValidElement<MarkdownCodeElementProps>(codeElement)) {
      return <Box my={3}>{children}</Box>;
    }

    // Extract language from className (format: "language-python")
    const { children: codeChildren, className } = codeElement.props;
    const match = /language-(?<lang>[-\w]+)/u.exec(className ?? "");
    const language = match?.groups?.lang;

    const codeText = Array.isArray(codeChildren)
      ? codeChildren.map((child) => (typeof child === "string" ? child : "")).join("")
      : typeof codeChildren === "string"
        ? codeChildren
        : "";

    const childString = codeText.replace(/\n$/u, "");

    return <MarkdownCodeBlock language={language} style={style} value={childString} />;
  };

const createMarkdownComponents = (style: SyntaxTheme): Components => ({
  // eslint-disable-next-line id-length
  a: LinkComponent,
  blockquote: BlockquoteComponent,
  code: InlineCodeComponent,
  del: DelComponent,
  em: EmComponent,
  h1: makeHeading("h1"),
  h2: makeHeading("h2"),
  h3: makeHeading("h3"),
  h4: makeHeading("h4"),
  h5: makeHeading("h5"),
  h6: makeHeading("h6"),
  hr: HrComponent,
  img: ImgComponent,
  li: LiComponent,
  ol: OlComponent,
  // eslint-disable-next-line id-length
  p: PComponent,
  pre: createPreComponent(style),
  table: TableComponent,
  tbody: Table.Body,
  td: Table.Cell,
  text: TextComponent,
  th: Table.ColumnHeader,
  thead: Table.Header,
  tr: Table.Row,
  ul: UlComponent,
});

const ReactMarkdown = ({ children, components: componentOverrides, ...restProps }: Options) => {
  const { colorMode } = useColorMode();
  const style = colorMode === "dark" ? oneDark : oneLight;
  const components = createMarkdownComponents(style);

  return (
    <Box alignSelf="stretch" css={markdownContentStyles} maxWidth="100%" minWidth={0} width="100%">
      <ReactMD
        components={{ ...components, ...componentOverrides }}
        {...restProps}
        remarkPlugins={[remarkGfm]}
        skipHtml
      >
        {children}
      </ReactMD>
    </Box>
  );
};

export default ReactMarkdown;
