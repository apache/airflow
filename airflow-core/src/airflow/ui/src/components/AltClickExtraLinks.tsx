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
import { useEffect, useRef, useState } from "react";

import { Box, Button, Portal, VStack } from "@chakra-ui/react";
import { useNavigate, type To } from "react-router-dom";

import type { ExtraLink } from "src/hooks/useTaskInstanceExtraLinks";

type Props = {
  readonly children: ReactElement;
  readonly links: ReadonlyArray<ExtraLink>;
  readonly navigateTo: To;
  readonly replace?: boolean;
};

type MenuPosition = {
  readonly x: number;
  readonly y: number;
};

const ExtraLinksMenu = ({
  links,
  onClose,
  position,
}: {
  readonly links: ReadonlyArray<ExtraLink>;
  readonly onClose: () => void;
  readonly position: MenuPosition;
}) => {
  const firstItemRef = useRef<HTMLAnchorElement | null>(null);

  useEffect(() => {
    firstItemRef.current?.focus();
  }, []);

  return (
    <Portal>
      <Box data-testid="extra-links-backdrop" inset={0} onClick={onClose} position="fixed" zIndex="modal" />
      <Box
        bg="bg.panel"
        borderRadius="md"
        boxShadow="lg"
        left={position.x}
        minW="180px"
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            onClose();
          }
        }}
        position="fixed"
        role="menu"
        top={position.y}
        zIndex="modal"
      >
        <VStack align="stretch" gap={0} p={1}>
          {links.map(({ name, url }, index) => (
            <Button asChild key={name} role="menuitem" variant="plain">
              <a
                href={url}
                onClick={onClose}
                ref={index === 0 ? firstItemRef : undefined}
                rel="noopener noreferrer"
                target="_blank"
              >
                {name}
              </a>
            </Button>
          ))}
        </VStack>
      </Box>
    </Portal>
  );
};

export const AltClickExtraLinks = ({ children, links, navigateTo, replace }: Props) => {
  const navigate = useNavigate();
  const [menuPosition, setMenuPosition] = useState<MenuPosition>();

  const handleClickCapture = (event: MouseEvent<HTMLDivElement>) => {
    if (!event.altKey || event.button !== 0) {
      return;
    }

    event.preventDefault();
    event.stopPropagation();

    if (links.length === 1) {
      const [link] = links;

      if (link !== undefined) {
        void globalThis.open(link.url, "_blank", "noopener");
      }
    } else if (links.length > 1) {
      setMenuPosition({ x: event.clientX, y: event.clientY });
    } else {
      void navigate(navigateTo, { replace });
    }
  };

  return (
    <Box display="contents" onClickCapture={handleClickCapture}>
      {children}
      {menuPosition === undefined ? undefined : (
        <ExtraLinksMenu links={links} onClose={() => setMenuPosition(undefined)} position={menuPosition} />
      )}
    </Box>
  );
};
