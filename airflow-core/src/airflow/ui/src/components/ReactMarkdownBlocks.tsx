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
import { Box, Flex, Spinner, Text } from "@chakra-ui/react";
import { renderToString as renderKatexToString } from "katex";
import { useEffect, useId, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";

import { LazyClipboard } from "src/components/ui";
import { useColorMode } from "src/context/colorMode";
import { renderMermaidDiagram } from "src/utils/renderMermaid";
import { SyntaxHighlighter, type SyntaxTheme } from "src/utils/syntaxHighlighter";

import { katexStyleLoader } from "./KatexStyleLoader";

const MarkdownBlockFrame = ({
  action,
  children,
  label,
}: {
  readonly action: ReactNode;
  readonly children: ReactNode;
  readonly label: string;
}) => (
  <Box maxWidth="100%" minWidth={0} my={3} width="100%">
    <Flex
      alignItems="center"
      bg="bg.muted"
      borderColor="border.emphasized"
      borderTopRadius="md"
      borderWidth="1px"
      justifyContent="space-between"
      minH={8}
      px={3}
      py={1}
    >
      <Text color="fg.muted" fontFamily="mono" fontSize="xs" lineHeight="short">
        {label}
      </Text>
      {action}
    </Flex>
    <Box
      borderBottomRadius="md"
      borderColor="border.emphasized"
      borderTopWidth={0}
      borderWidth="1px"
      maxWidth="100%"
      minWidth={0}
      overflow="hidden"
    >
      {children}
    </Box>
  </Box>
);

const MarkdownPlainCodeBlock = ({
  language,
  style,
  value,
}: {
  readonly language?: string;
  readonly style: SyntaxTheme;
  readonly value: string;
}) => {
  const { t: translate } = useTranslation("components");

  const codeBlockStyle = style['pre[class*="language-"]'];

  return (
    <MarkdownBlockFrame
      action={
        <LazyClipboard
          aria-label={translate("clipboard.copy")}
          data-testid="markdown-copy-button"
          getValue={() => value}
          title={translate("clipboard.copy")}
        />
      }
      label={language ?? "text"}
    >
      <Box
        css={{ ...codeBlockStyle, borderRadius: 0, margin: 0 }}
        data-testid="markdown-code-scroll-area"
        maxWidth="100%"
        minWidth={0}
        overflowX="auto"
        overflowY="hidden"
        width="100%"
      >
        <Box data-testid="markdown-code-content" display="inline-block" minWidth="100%">
          <SyntaxHighlighter
            codeTagProps={{
              style: {
                background: "transparent",
                overflowWrap: "normal",
                whiteSpace: "pre",
                wordBreak: "normal",
              },
            }}
            customStyle={{
              background: "transparent",
              borderRadius: 0,
              margin: 0,
              padding: 0,
              width: "max-content",
            }}
            language={language}
            lineNumberStyle={{ minWidth: "2.5em", opacity: 0.6, paddingRight: "1em" }}
            PreTag="div"
            showLineNumbers
            style={style}
          >
            {value}
          </SyntaxHighlighter>
        </Box>
      </Box>
    </MarkdownBlockFrame>
  );
};

const MarkdownMathBlock = ({ style, value }: { readonly style: SyntaxTheme; readonly value: string }) => {
  const { t: translate } = useTranslation("components");

  useEffect(() => {
    void katexStyleLoader.load();
  }, []);

  try {
    const markup = renderKatexToString(value, { displayMode: true, throwOnError: true });

    return (
      <MarkdownBlockFrame
        action={
          <LazyClipboard
            aria-label={translate("clipboard.copy")}
            data-testid="markdown-copy-button"
            getValue={() => value}
            title={translate("clipboard.copy")}
          />
        }
        label="math"
      >
        <Box data-testid="markdown-math-content" maxWidth="100%" minWidth={0} overflowX="auto" p={3}>
          <Box
            /* Trusting KaTeX to escape user content in the generated HTML */
            dangerouslySetInnerHTML={{ __html: markup }}
          />
        </Box>
      </MarkdownBlockFrame>
    );
  } catch {
    return <MarkdownPlainCodeBlock language="math" style={style} value={value} />;
  }
};

export const MarkdownMermaid = ({
  chart,
  fallbackStyle,
}: {
  readonly chart: string;
  readonly fallbackStyle: SyntaxTheme;
}) => {
  const { t: translate } = useTranslation("components");
  const { colorMode } = useColorMode();
  const diagramId = useId().replaceAll(":", "");
  const [error, setError] = useState(false);
  const [svg, setSvg] = useState<string>();
  const theme = colorMode === "dark" ? "dark" : "default";

  useEffect(() => {
    let cancelled = false;

    const renderMermaid = async () => {
      try {
        const renderedSvg = await renderMermaidDiagram({
          chart,
          diagramId: `markdown-mermaid-${diagramId}`,
          theme,
        });

        if (!cancelled) {
          setSvg(renderedSvg);
          setError(false);
        }
      } catch {
        if (!cancelled) {
          setError(true);
          setSvg(undefined);
        }
      }
    };

    void renderMermaid();

    return () => {
      cancelled = true;
    };
  }, [chart, diagramId, theme]);

  if (error) {
    return <MarkdownPlainCodeBlock language="mermaid" style={fallbackStyle} value={chart} />;
  }

  return (
    <MarkdownBlockFrame
      action={
        <LazyClipboard
          aria-label={translate("clipboard.copy")}
          data-testid="markdown-mermaid-copy-button"
          getValue={() => chart}
          title={translate("clipboard.copy")}
        />
      }
      label="mermaid"
    >
      <Box maxWidth="100%" minHeight="8rem" minWidth={0} overflow="hidden" p={3} width="100%">
        {svg === undefined ? (
          <Box
            alignItems="center"
            color="fg.muted"
            data-testid="markdown-mermaid-loading"
            display="inline-flex"
            minHeight="2rem"
          >
            <Spinner size="sm" />
          </Box>
        ) : (
          <Box
            css={{
              "& svg": {
                display: "block",
                height: "auto",
                marginInline: "auto",
                maxWidth: "100%",
                width: "100%",
              },
            }}
            /* Trusting mermaid's strict-mode sanitizer */
            dangerouslySetInnerHTML={{ __html: svg }}
            data-testid="markdown-mermaid-diagram"
          />
        )}
      </Box>
    </MarkdownBlockFrame>
  );
};

export const MarkdownCodeBlock = ({
  language,
  style,
  value,
}: {
  readonly language?: string;
  readonly style: SyntaxTheme;
  readonly value: string;
}) => {
  if (language === "math") {
    return <MarkdownMathBlock style={style} value={value} />;
  }

  if (language === "mermaid") {
    return <MarkdownMermaid chart={value} fallbackStyle={style} />;
  }

  return <MarkdownPlainCodeBlock language={language} style={style} value={value} />;
};
