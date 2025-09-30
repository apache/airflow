/* eslint-disable i18next/no-literal-string */

/* eslint-disable unicorn/consistent-function-scoping */

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
import { Box, Container, Heading, HStack, VStack } from "@chakra-ui/react";
import { useState } from "react";

import {
  AirflowComponents,
  Buttons,
  Charts,
  Collections,
  Colors,
  Forms,
  Graph,
  Layout,
  ModalDialog,
  Overlays,
  Feedback,
  TableOfContents,
  Typography,
} from "./index";

/**
 * AccessibilityHelper page - A comprehensive component showcase inspired by Chakra UI Playground
 * for testing accessibility, contrast ratios, and Lighthouse metrics.
 *
 * Based on: https://www.chakra-ui.com/playground
 * This page includes examples of all major UI components used in the Airflow UI
 * to ensure they meet accessibility standards and provide good contrast ratios.
 */
export const Playground = () => {
  // Modal state
  const [isDialogOpen, setIsDialogOpen] = useState(false);

  // Form state
  const [formState, setFormState] = useState({
    currentPage: 1,
    multipleSegmented: ["option1", "option2"],
    progress: 75,
    radio: "option1",
    radioCard: "option1",
    segmented: ["option1"],
    slider: [50],
    switch: false,
  });

  // Section visibility state - all open by default for better UX
  const [openSections, setOpenSections] = useState<Record<string, boolean>>({
    airflow: true,
    buttons: true,
    charts: true,
    collections: true,
    colors: true,
    forms: true,
    graphs: true,
    layout: true,
    overlays: true,
    progress: true,
    typography: true,
  });

  const toggleSection = (section: keyof typeof openSections) => {
    setOpenSections((prev) => ({ ...prev, [section]: !prev[section] }));
  };

  const scrollToSection = (sectionId: string) => {
    document.querySelector(`#${sectionId}`)?.scrollIntoView({ behavior: "smooth" });
  };

  const updateFormState = (updates: Partial<typeof formState>) => {
    setFormState((prev) => ({ ...prev, ...updates }));
  };

  return (
    <Container>
      <VStack align="stretch" gap={10}>
        {/* Page Header */}
        <Box as="header" mt={6} textAlign="center">
          <Heading as="h1" id="main-heading" size="2xl">
            Airflow UI Component Playground
          </Heading>
        </Box>

        {/* Two Column Layout: Content + Table of Contents */}
        <HStack align="flex-start" gap={8}>
          {/* Main Content */}
          <Box flex="1">
            <VStack align="stretch" gap={8}>
              {/* Colors Section */}
              <Colors isOpen={openSections.colors ?? true} onToggle={() => toggleSection("colors")} />

              {/* Airflow Components Section */}
              <AirflowComponents isOpen={openSections.airflow ?? true} onToggle={() => toggleSection("airflow")} />

              {/* Layout Section */}
              <Layout isOpen={openSections.layout ?? true} onToggle={() => toggleSection("layout")} />

              {/* Typography Section */}
              <Typography
                isOpen={openSections.typography ?? true}
                onToggle={() => toggleSection("typography")}
              />

              {/* Buttons Section */}
              <Buttons isOpen={openSections.buttons ?? true} onToggle={() => toggleSection("buttons")} />

              {/* Forms Section */}
              <Forms
                formState={formState}
                isOpen={openSections.forms ?? true}
                onToggle={() => toggleSection("forms")}
                updateFormState={updateFormState}
              />

              {/* Collections Section */}
              <Collections
                isOpen={openSections.collections ?? true}
                onToggle={() => toggleSection("collections")}
              />

              {/* Progress & Alerts Section */}
              <Feedback
                isProgressOpen={openSections.progress ?? true}
                isStatesOpen={false}
                onProgressToggle={() => toggleSection("progress")}
                onStatesToggle={() => {}}
                progressValue={formState.progress}
                setProgressValue={(value: number) => updateFormState({ progress: value })}
              />

              {/* Graph Components Section */}
              <Graph isOpen={openSections.graphs ?? true} onToggle={() => toggleSection("graphs")} />

              {/* Charts & Gantt Section */}
              <Charts isOpen={openSections.charts ?? true} onToggle={() => toggleSection("charts")} />

              {/* Overlays Section */}
              <Overlays isOpen={openSections.overlays ?? true} onToggle={() => toggleSection("overlays")} />
            </VStack>
          </Box>

          {/* Table of Contents Sidebar */}
          <TableOfContents
            openSections={openSections}
            scrollToSection={scrollToSection}
            setOpenSections={setOpenSections}
          />
        </HStack>

        {/* Modal Dialog */}
        <ModalDialog isOpen={isDialogOpen} onClose={() => setIsDialogOpen(false)} />
      </VStack>
    </Container>
  );
};
