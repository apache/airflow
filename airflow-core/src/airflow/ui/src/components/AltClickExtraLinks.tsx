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
import type { MouseEvent, ReactElement } from "react";
import { useRef, useState } from "react";

import { Box, Menu } from "@chakra-ui/react";
import { useNavigate, type To } from "react-router-dom";

import type { ExtraLink } from "src/hooks/useTaskInstanceExtraLinks";

type Props = {
  readonly children: ReactElement;
  readonly links: ReadonlyArray<ExtraLink>;
  readonly navigateTo: To;
  readonly replace?: boolean;
};

type AnchorPoint = {
  readonly x: number;
  readonly y: number;
};

export const AltClickExtraLinks = ({ children, links, navigateTo, replace }: Props) => {
  const navigate = useNavigate();
  const [anchorPoint, setAnchorPoint] = useState<AnchorPoint>();
  const isAltClick = useRef(false);

  const handleClickCapture = (event: MouseEvent<HTMLDivElement>) => {
    if (!event.altKey || event.button !== 0) {
      isAltClick.current = false;

      return;
    }

    isAltClick.current = true;
    queueMicrotask(() => {
      isAltClick.current = false;
    });
    event.preventDefault();
    event.stopPropagation();

    if (links.length === 1) {
      const [link] = links;

      if (link !== undefined) {
        void globalThis.open(link.url, "_blank", "noopener");
      }
    } else if (links.length > 1) {
      setAnchorPoint({ x: event.clientX, y: event.clientY });
    } else {
      void navigate(navigateTo, { replace });
    }
  };

  return (
    <Menu.Root
      anchorPoint={anchorPoint}
      aria-label="Extra links"
      onOpenChange={({ open }) => {
        if (open && !isAltClick.current) {
          return;
        }
        if (!open) {
          setAnchorPoint(undefined);
        }
      }}
      open={anchorPoint !== undefined}
      positioning={{ placement: "bottom-start" }}
    >
      <Menu.Trigger asChild>
        <Box display="contents" onClickCapture={handleClickCapture}>
          {children}
        </Box>
      </Menu.Trigger>
      <Menu.Positioner>
        <Menu.Content>
          {links.map(({ name, url }) => (
            <Menu.Item asChild key={name} value={name}>
              <a href={url} rel="noopener noreferrer" target="_blank">
                {name}
              </a>
            </Menu.Item>
          ))}
        </Menu.Content>
      </Menu.Positioner>
    </Menu.Root>
  );
};
