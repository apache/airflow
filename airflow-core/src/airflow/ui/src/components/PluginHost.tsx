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
import { Suspense, lazy } from "react";
import { useParams } from "react-router-dom";

import { ErrorAlert } from "src/components/ErrorAlert";
import { useUiPlugins } from "src/queries/useUiPlugins";

type IframeProps = {
  readonly src: string;
};

const Iframe = ({ src }: IframeProps) => (
  <Box height="calc(100vh - 80px)" width="100%">
    <iframe
      sandbox="allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox"
      src={src}
      style={{
        border: "none",
        height: "100%",
        width: "100%",
      }}
      title="Plugin Content"
    />
  </Box>
);

export const PluginHost = () => {
  const { slug } = useParams<{ slug: string }>();
  const { data, error, isLoading } = useUiPlugins();

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
    return <Iframe src={plugin.entry} />;
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
