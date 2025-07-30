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
import { ChakraProvider } from "@chakra-ui/react";
import { QueryClientProvider } from "@tanstack/react-query";
import { ThemeProvider } from "next-themes";
import React from "react";
import { CookiesProvider } from "react-cookie";
import { createRoot } from "react-dom/client";
import { RouterProvider } from "react-router-dom";

import { router } from "src/router";
import { system } from "src/theme";

import { queryClient } from "./queryClient";

// Add global accessibility styles
const globalStyles = `
  .sr-only {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0, 0, 0, 0);
    white-space: nowrap;
    border: 0;
  }

  /* Ensure focus is visible for keyboard navigation */
  *:focus-visible {
    outline: 2px solid #005fcc;
    outline-offset: 2px;
  }

  /* Reduce motion for users who prefer it */
  @media (prefers-reduced-motion: reduce) {
    *,
    *::before,
    *::after {
      animation-duration: 0.01ms !important;
      animation-iteration-count: 1 !important;
      transition-duration: 0.01ms !important;
    }
  }

  /* High contrast mode support */
  @media (prefers-contrast: high) {
    * {
      text-shadow: none !important;
      box-shadow: none !important;
    }
  }
`;

// Inject global styles
const styleSheet = document.createElement("style");

styleSheet.type = "text/css";
styleSheet.innerHTML = globalStyles;
document.head.append(styleSheet);

type ErrorBoundaryProps = {
  readonly children: React.ReactNode;
}

type ErrorBoundaryState = {
  hasError: boolean;
}

// Error boundary component for better error handling
class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError(_error: Error): ErrorBoundaryState {
    return { hasError: true };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error("Application error:", error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div
          style={{
            backgroundColor: "#f8d7da",
            border: "1px solid #f5c6cb",
            borderRadius: "4px",
            color: "#721c24",
            fontFamily: "Arial, sans-serif",
            margin: "20px",
            padding: "50px",
            textAlign: "center",
          }}
        >
          <h1>Something went wrong</h1>
          <p>Please refresh the page and try again.</p>
          <button
            onClick={() => window.location.reload()}
            style={{
              backgroundColor: "#dc3545",
              border: "none",
              borderRadius: "4px",
              color: "white",
              cursor: "pointer",
              marginTop: "10px",
              padding: "10px 20px",
            }}
          >
            Reload Page
          </button>
        </div>
      );
    }

    return this.props.children;
  }
}

const rootElement = document.querySelector("#root") as HTMLDivElement;

if (!rootElement) {
  throw new Error("Root element not found");
}

createRoot(rootElement).render(
  <ErrorBoundary>
    <ChakraProvider value={system}>
      <ThemeProvider attribute="class" disableTransitionOnChange>
        <QueryClientProvider client={queryClient}>
          <CookiesProvider>
            <RouterProvider router={router} />
          </CookiesProvider>
        </QueryClientProvider>
      </ThemeProvider>
    </ChakraProvider>
  </ErrorBoundary>,
);
