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
import { useCallback, useEffect, useId, useRef, useState, type PointerEvent, type ReactNode } from "react";

import { Menu } from "src/system-components";

// Opening the menu moves focus into its content, so require a short dwell: a pointer
// sweeping across the sidebar on its way elsewhere must not pop menus open.
const OPEN_DELAY_MS = 150;
// The content is portalled and offset from the button, so leave a grace period for the
// pointer to cross the gap between the two.
const CLOSE_DELAY_MS = 250;

const MENU_SELECTOR = '[data-scope="menu"]';

// The sidebar menus behave as one menubar even though each owns its open state. Opening one
// has to close the others synchronously: a menu left to close on its own delay afterwards
// pulls focus back to its own trigger, which dismisses the menu the pointer moved on to.
// A module-level group is enough, since the Nav is mounted once.
const group: {
  closeOthers: Map<string, () => void>;
  // Where focus was before the pointer opened the first menu of a pass, so a hover the user
  // never meant as an interaction does not steal focus from what they were typing in. It
  // belongs to the group, not a menu: a pass may hand off several times before focus returns.
  focusBeforeHover: Element | undefined;
  openId: string | undefined;
} = {
  closeOthers: new Map(),
  focusBeforeHover: undefined,
  openId: undefined,
};

type NavMenuProps = {
  readonly children: ReactNode;
  readonly trigger: ReactNode;
};

/**
 * A sidebar menu that opens when the pointer rests on its NavButton. Click, keyboard and
 * touch still work through the underlying menu, so a trigger that is itself a link keeps
 * navigating on click.
 */
export const NavMenu = ({ children, trigger }: NavMenuProps) => {
  const id = useId();
  const [open, setOpen] = useState(false);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  const cancelPendingChange = useCallback(() => {
    if (timeoutRef.current !== undefined) {
      clearTimeout(timeoutRef.current);
      timeoutRef.current = undefined;
    }
  }, []);

  const closeMenu = useCallback(() => {
    cancelPendingChange();
    if (group.openId === id) {
      group.openId = undefined;
    }
    setOpen(false);
  }, [cancelPendingChange, id]);

  const openMenu = useCallback(() => {
    if (group.openId === undefined) {
      group.focusBeforeHover = document.activeElement ?? undefined;
    }
    group.openId = id;
    group.closeOthers.forEach((closeOther, otherId) => {
      if (otherId !== id) {
        closeOther();
      }
    });
    setOpen(true);
  }, [id]);

  useEffect(() => {
    group.closeOthers.set(id, closeMenu);

    return () => {
      group.closeOthers.delete(id);
      if (group.openId === id) {
        group.openId = undefined;
      }
      cancelPendingChange();
    };
  }, [cancelPendingChange, closeMenu, id]);

  // The menu focuses its trigger as it closes; undo that only when the menu itself still
  // holds focus, so focus moved by the user — or by the next menu — is left alone.
  const restoreFocusBeforeHover = useCallback(() => {
    const target = group.focusBeforeHover;

    group.focusBeforeHover = undefined;

    if (!(target instanceof HTMLElement) || !target.isConnected) {
      return;
    }

    requestAnimationFrame(() => {
      if (group.openId !== undefined || document.activeElement?.closest(MENU_SELECTOR) === null) {
        return;
      }
      target.focus({ preventScroll: true });
    });
  }, []);

  const hoverProps = {
    onPointerEnter: (event: PointerEvent) => {
      if (event.pointerType !== "mouse") {
        return;
      }
      cancelPendingChange();
      // Hand off straight away when the pointer is already travelling between open menus;
      // only the first menu of a pass has to earn its dwell.
      if (group.openId !== undefined && group.openId !== id) {
        openMenu();

        return;
      }
      timeoutRef.current = setTimeout(openMenu, OPEN_DELAY_MS);
    },
    onPointerLeave: (event: PointerEvent) => {
      if (event.pointerType !== "mouse") {
        return;
      }
      cancelPendingChange();
      timeoutRef.current = setTimeout(() => {
        closeMenu();
        restoreFocusBeforeHover();
      }, CLOSE_DELAY_MS);
    },
  };

  return (
    <Menu.Root
      onOpenChange={({ open: isOpen }) => {
        if (isOpen) {
          openMenu();
        } else {
          closeMenu();
        }
        // Clicks, Escape and item selection own focus from here on.
        group.focusBeforeHover = undefined;
      }}
      open={open}
      positioning={{ placement: "right" }}
    >
      <Menu.Trigger asChild {...hoverProps}>
        {trigger}
      </Menu.Trigger>
      <Menu.Content {...hoverProps}>{children}</Menu.Content>
    </Menu.Root>
  );
};
