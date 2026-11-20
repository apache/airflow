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
import { Component, type ErrorInfo, type ReactNode } from "react";

type Props = {
  readonly children: ReactNode;
  /** Named in the console message, so the failing plugin is identifiable. */
  readonly name: string;
  /** A failed plugin renders again when this changes, e.g. on navigating to another Dag. */
  readonly resetKey?: string;
};

/**
 * Keeps a plugin component's render failure inside the plugin's own slot.
 *
 * Without this the error reaches the route's error element and replaces the whole page, so one
 * plugin takes down the page it was only meant to add to. A plausible way in: a plugin named
 * `History` or `Image` finds the browser's own constructor already on `globalThis`, which shadows
 * the bundle's own registration, and React throws calling it.
 *
 * Renders nothing once it has caught, which matches how a bundle that fails to load behaves: the
 * page shows as it would without the plugin. A class is required: there is still no hook
 * equivalent of `componentDidCatch`.
 */
export class PluginBoundary extends Component<Props, { hasError: boolean }> {
  public constructor(props: Props) {
    super(props);
    this.state = { hasError: false };
  }

  public static getDerivedStateFromError() {
    return { hasError: true };
  }

  public componentDidCatch(error: Error, info: ErrorInfo) {
    const { name } = this.props;

    // eslint-disable-next-line no-console
    console.error(`Plugin "${name}" failed to render`, error, info.componentStack);
  }

  public componentDidUpdate(prevProps: Props) {
    const { resetKey } = this.props;
    const { hasError } = this.state;

    if (hasError && prevProps.resetKey !== resetKey) {
      this.setState({ hasError: false });
    }
  }

  public render() {
    const { children } = this.props;
    const { hasError } = this.state;

    return hasError ? undefined : children;
  }
}
