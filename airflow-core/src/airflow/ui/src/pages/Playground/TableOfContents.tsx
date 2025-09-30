/* eslint-disable i18next/no-literal-string */

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
import { Box, ButtonGroup, Heading, HStack, IconButton, Link, List, Text, VStack } from "@chakra-ui/react";
import React from "react";
import { MdExpand, MdCompress } from "react-icons/md";

type TableOfContentsProps = {
  readonly openSections: Record<string, boolean>;
  readonly scrollToSection: (sectionId: string) => void;
  readonly setOpenSections: React.Dispatch<React.SetStateAction<Record<string, boolean>>>;
};

const sections = [
  { id: "colors", title: "Colors" },
  { id: "airflow", title: "Airflow Components" },
  { id: "layout", title: "Layout" },
  { id: "typography", title: "Typography" },
  { id: "buttons", title: "Buttons" },
  { id: "forms", title: "Forms" },
  { id: "collections", title: "Collections" },
  { id: "progress", title: "Progress & Alerts" },
  { id: "graphs", title: "Graph" },
  { id: "charts", title: "Charts" },
  { id: "overlays", title: "Overlays" },
];

export const TableOfContents = ({ openSections, scrollToSection, setOpenSections }: TableOfContentsProps) => (
  <Box height="fit-content" position="sticky" top="4">
    <Box bg="bg.panel" borderColor="border.muted" borderWidth="1px" padding="4">
      <VStack align="stretch" gap="4">
        <Heading size="lg">Table of Contents</Heading>
        {/* Quick Actions */}
        <VStack align="stretch" gap="2">
          <Text color="fg.muted" fontSize="xs" fontWeight="semibold">
            Quick Actions
          </Text>
          <ButtonGroup attached size="sm" variant="outline" width="full">
            <IconButton
              aria-label="Expand All"
              onClick={() => {
                Object.keys(openSections).forEach((section) => {
                  setOpenSections((prev) => ({ ...prev, [section]: true }));
                });
              }}
              size="sm"
              title="Expand All"
            >
              <MdExpand />
            </IconButton>
            <IconButton
              aria-label="Collapse All"
              onClick={() => {
                Object.keys(openSections).forEach((section) => {
                  setOpenSections((prev) => ({ ...prev, [section]: false }));
                });
              }}
              size="sm"
              title="Collapse All"
            >
              <MdCompress />
            </IconButton>
          </ButtonGroup>
        </VStack>

        {/* Navigation Links */}
        <VStack align="stretch" gap="3">
          <Text color="fg.muted" fontSize="xs" fontWeight="semibold">
            Sections
          </Text>
          <Box aria-label="Page sections navigation" as="nav">
            <List.Root variant="plain">
              {sections.map(({ id, title }) => (
                <List.Item key={id}>
                  <Link
                    _hover={{ bg: "bg.muted", textDecoration: "none" }}
                    color="fg.default"
                    display="block"
                    fontSize="sm"
                    onClick={() => {
                      // Toggle the section state (open/close)
                      setOpenSections((prev) => ({ ...prev, [id]: !openSections[id] }));
                      scrollToSection(id);
                    }}
                    padding="2"
                    transition="background 0.2s"
                    width="full"
                  >
                    <HStack gap="2" justify="space-between">
                      <Text>{title}</Text>
                      <Text color="brand.solid" fontSize="lg">
                        {(openSections[id] ?? true) ? "−" : "+"}
                      </Text>
                    </HStack>
                  </Link>
                </List.Item>
              ))}
            </List.Root>
          </Box>
        </VStack>
      </VStack>
    </Box>
  </Box>
);
