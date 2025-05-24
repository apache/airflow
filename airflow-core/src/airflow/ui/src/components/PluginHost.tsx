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
import { Box, Center, Heading, Spinner, Text } from "@chakra-ui/react";
import { Suspense, lazy, useEffect, useRef } from "react";
import { useParams } from "react-router-dom";

import { ErrorAlert } from "src/components/ErrorAlert";
import { useColorMode } from "src/context/colorMode";
import { useUiPlugins } from "src/queries/useUiPlugins";

type IframeProps = {
  readonly colorMode: "dark" | "light" | undefined;
  readonly src: string;
};

const Iframe = ({ colorMode, src }: IframeProps) => {
  const iframeRef = useRef<HTMLIFrameElement>(null);

  // Add theme parameter to URL
  const urlWithTheme = new URL(src, globalThis.location.origin);

  urlWithTheme.searchParams.set("theme", colorMode ?? "light");

  useEffect(() => {
    const iframe = iframeRef.current;

    if (!iframe) {
      return undefined;
    }

    const handleLoad = () => {
      // Send theme information to iframe after it loads
      iframe.contentWindow?.postMessage(
        {
          theme: colorMode ?? "light",
          type: "AIRFLOW_THEME_UPDATE",
        },
        "*",
      );
    };

    iframe.addEventListener("load", handleLoad);

    return () => {
      iframe.removeEventListener("load", handleLoad);
    };
  }, [colorMode]);

  // Send theme updates when colorMode changes
  useEffect(() => {
    const iframe = iframeRef.current;

    if (!iframe?.contentWindow) {
      return undefined;
    }

    iframe.contentWindow.postMessage(
      {
        theme: colorMode ?? "light",
        type: "AIRFLOW_THEME_UPDATE",
      },
      "*",
    );

    return undefined;
  }, [colorMode]);

  return (
    <Box height="calc(100vh - 80px)" width="100%">
      <iframe
        ref={iframeRef}
        sandbox="allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox"
        src={urlWithTheme.toString()}
        style={{
          border: "none",
          height: "100%",
          width: "100%",
        }}
        title="Plugin Content"
      />
    </Box>
  );
};

export const PluginHost = () => {
  const { slug } = useParams<{ slug: string }>();
  const { data, error, isLoading } = useUiPlugins();
  const { colorMode } = useColorMode();

  if (isLoading) {
    return (
      <Center height="200px">
        <Spinner size="lg" />
      </Center>
    );
  }

  if (error) {
    return <ErrorAlert error={error} />;
  }

  const plugin = data?.plugins.find((pluginItem) => pluginItem.slug === slug);

  if (!plugin) {
    return (
      <Box p={5}>
        <Heading size="md">Plugin Not Found</Heading>
        <Text mt={2}>
          The plugin &quot;{slug}&quot; was not found or you don&apos;t have permission to access it.
        </Text>
      </Box>
    );
  }

  if (plugin.type === "iframe") {
    return <Iframe colorMode={colorMode} src={plugin.entry} />;
  }

  // Module federation is not implemented yet
  try {
    // This is a placeholder for module federation
    // In a real implementation, this would use a more sophisticated approach
    const RemoteComponent = lazy(() => import(/* @vite-ignore */ plugin.entry));

    return (
      <Suspense fallback={<Spinner />}>
        <RemoteComponent />
      </Suspense>
    );
  } catch {
    return (
      <Box p={5}>
        <Heading size="md">Error Loading Module</Heading>
        <Text mt={2}>There was an error loading the module for plugin &quot;{plugin.label}&quot;.</Text>
        <Text mt={2}>Module federation is not fully implemented yet. Please use iframe type instead.</Text>
      </Box>
    );
  }
};
