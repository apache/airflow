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

const createStyle = (bundle: AnserJsonEntry): React.CSSProperties => {
  const style: React.CSSProperties = {};

  if (bundle.bg) {
    style.backgroundColor = `rgb(${bundle.bg})`;
  }
  if (bundle.fg) {
    style.color = `rgb(${bundle.fg})`;
  }

  switch (bundle.decoration) {
    case "blink":
      style.textDecoration = "blink";
      break;
    case "bold":
      style.fontWeight = "bold";
      break;
    case "dim":
      style.opacity = "0.5";
      break;
    case "hidden":
      style.visibility = "hidden";
      break;
    case "italic":
      style.fontStyle = "italic";
      break;
    case "reverse":
      break;
    case "strikethrough":
      style.textDecoration = "line-through";
      break;
    case "underline":
      style.textDecoration = "underline";
      break;
    // eslint-disable-next-line unicorn/no-useless-switch-case
    case null:
    default:
      break;
  }

  return style;
};

const convertBundleIntoReact = (options: {
  bundle: AnserJsonEntry;
  key: number;
  linkify: boolean;
  useClasses: boolean;
}): JSX.Element => {
  const { bundle, key, linkify, useClasses } = options;
  const style = useClasses ? undefined : createStyle(bundle);
  const className = useClasses ? createClass(bundle) : undefined;

  if (!linkify) {
    return React.createElement("span", { className, key, style }, bundle.content);
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
      React.createElement(
        "a",
        {
          href,
          key: index,
          target: "_blank",
        },
        url,
      ),
    );

    index = linkRegex.lastIndex;
  }

  if (index < bundle.content.length) {
    content.push(bundle.content.slice(index));
  }

  return React.createElement("span", { className, key, style }, content);
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
}) =>
  React.createElement(
    "code",
    { className },
    ansiToJSON(children, useClasses).map((bundle, index) =>
      convertBundleIntoReact({ bundle, key: index, linkify, useClasses }),
    ),
  );

export default AnsiRenderer;
