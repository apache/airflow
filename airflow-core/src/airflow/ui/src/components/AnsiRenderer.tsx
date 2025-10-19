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
import { chakra } from "@chakra-ui/react";
import Anser, { type AnserJsonEntry } from "anser";
import * as React from "react";

const fixBackspace = (inputText: string): string => {
  let tmp = inputText;

  do {
    const previous = tmp;

    // eslint-disable-next-line no-control-regex
    tmp = tmp.replaceAll(/[^\n]\u0008/gmu, "");
    if (tmp.length >= previous.length) {
      break;
    }
  } while (tmp.length > 0);

  return tmp;
};

const ansiToJSON = (input: string, useClasses: boolean = false): Array<AnserJsonEntry> => {
  const processedInput = fixBackspace(input.replaceAll("\r", ""));

  return Anser.ansiToJson(processedInput, {
    json: true,
    remove_empty: true,
    use_classes: useClasses,
  });
};

const createClass = (bundle: AnserJsonEntry): string | undefined => {
  let classNames = "";

  if (bundle.bg) {
    classNames += `${bundle.bg}-bg `;
  }
  if (bundle.fg) {
    classNames += `${bundle.fg}-fg `;
  }
  if (bundle.decoration) {
    classNames += `ansi-${bundle.decoration} `;
  }

  if (classNames === "") {
    return undefined;
  }

  return classNames.slice(0, classNames.length - 1);
};

// Map RGB values to Chakra UI semantic tokens
// These are the standard ANSI color RGB values that anser library outputs
const rgbToChakraColorMap: Record<string, string> = {
  "0, 0, 0": "gray.900", // Black (30m)
  "0, 0, 187": "blue.fg", // Blue (34m)
  "0, 187, 0": "green.fg", // Green (32m)
  "0, 187, 187": "cyan.fg", // Cyan (36m)
  "0, 255, 0": "green.400", // Bright Green (92m)
  "85, 85, 85": "black", // Bright Black/Gray (90m)
  "85, 85, 255": "blue.400", // Bright Blue (94m)
  "85, 255, 255": "cyan.400", // Bright Cyan (96m)
  "187, 0, 0": "red.fg", // Red (31m)
  "187, 0, 187": "purple.fg", // Magenta (35m)
  "187, 187, 0": "yellow.fg", // Yellow (33m)
  "187, 187, 187": "gray.100", // White (37m)
  "255, 85, 85": "red.400", // Bright Red (91m)
  "255, 85, 255": "purple.400", // Bright Magenta (95m)
  "255, 255, 85": "yellow.400", // Bright Yellow (93m)
  "255, 255, 255": "white", // Bright White (97m)
};

const createChakraProps = (bundle: AnserJsonEntry) => {
  const props: Record<string, number | string> = {};

  // Handle background colors
  if (bundle.bg) {
    const bgColor = rgbToChakraColorMap[bundle.bg];

    props.bg = bgColor ?? `rgb(${bundle.bg})`;
  }

  // Handle foreground colors
  if (bundle.fg) {
    const fgColor = rgbToChakraColorMap[bundle.fg];

    props.color = fgColor ?? `rgb(${bundle.fg})`;
  }

  // Handle text decorations
  switch (bundle.decoration) {
    case "blink":
      props.textDecoration = "blink";
      break;
    case "bold":
      props.fontWeight = "bold";
      break;
    case "dim":
      props.opacity = "0.5";
      break;
    case "hidden":
      props.visibility = "hidden";
      break;
    case "italic":
      props.fontStyle = "italic";
      break;
    case "reverse":
      // Could implement reverse video if needed
      break;
    case "strikethrough":
      props.textDecoration = "line-through";
      break;
    case "underline":
      props.textDecoration = "underline";
      break;
    // eslint-disable-next-line unicorn/no-useless-switch-case
    case null:
    default:
      break;
  }

  return props;
};

const convertBundleIntoReact = (options: {
  bundle: AnserJsonEntry;
  key: number;
  linkify: boolean;
  useClasses: boolean;
}): JSX.Element => {
  const { bundle, key, linkify, useClasses } = options;
  const style = useClasses ? undefined : createChakraProps(bundle);
  const className = useClasses ? createClass(bundle) : undefined;

  if (!linkify) {
    if (useClasses) {
      return (
        <span className={className} key={key}>
          {bundle.content}
        </span>
      );
    }

    return (
      <chakra.span key={key} {...style}>
        {bundle.content}
      </chakra.span>
    );
  }

  const content: Array<React.ReactNode> = [];
  const linkRegex =
    /(?<whitespace>\s|^)(?<url>https?:\/\/(?:www\.|(?!www))[^\s.]+\.\S{2,}|www\.\S+\.\S{2,})/gu;

  let index = 0;
  let match: RegExpExecArray | null;

  while ((match = linkRegex.exec(bundle.content)) !== null) {
    const { groups } = match;
    const pre = groups?.whitespace ?? "";
    const url = groups?.url ?? "";
    const startIndex = match.index + pre.length;

    if (startIndex > index) {
      content.push(bundle.content.slice(index, startIndex));
    }

    const href = url.startsWith("www.") ? `http://${url}` : url;

    content.push(
      <a href={href} key={index} rel="noreferrer" target="_blank">
        {url}
      </a>,
    );

    index = linkRegex.lastIndex;
  }

  if (index < bundle.content.length) {
    content.push(bundle.content.slice(index));
  }

  if (useClasses) {
    return (
      <span className={className} key={key}>
        {content}
      </span>
    );
  }

  return (
    <chakra.span key={key} {...style}>
      {content}
    </chakra.span>
  );
};

type AnsiRendererProps = {
  readonly children?: string;
  readonly className?: string;
  readonly linkify?: boolean;
  readonly useClasses?: boolean;
};

export const AnsiRenderer: React.FC<AnsiRendererProps> = ({
  children = "",
  className,
  linkify = false,
  useClasses = false,
}) => (
  <code className={className}>
    {ansiToJSON(children, useClasses).map((bundle, index) =>
      convertBundleIntoReact({ bundle, key: index, linkify, useClasses }),
    )}
  </code>
);

export default AnsiRenderer;
