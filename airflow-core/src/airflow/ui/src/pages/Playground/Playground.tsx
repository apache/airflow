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
  BadgesAvatarsSection,
  ButtonsCodeSection,
  ChartsGanttSection,
  ColorPaletteSection,
  FormsInputsSection,
  GraphSection,
  ModalDialog,
  ProgressTaskStatesSection,
  TableOfContents,
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
    progress: 75,
    radio: "option1",
    slider: [50],
    switch: false,
  });

  // Section visibility state - all open by default for better UX
  const [openSections, setOpenSections] = useState<Record<string, boolean>>({
    badges: true,
    buttons: true,
    charts: true,
    colors: true,
    forms: true,
    graphs: true,
    progress: true,
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
              {/* Color Palette Section */}
              <ColorPaletteSection
                isOpen={openSections.colors ?? true}
                onToggle={() => toggleSection("colors")}
              />

              {/* Components Section */}
              <Box id="components">
                <VStack align="stretch" gap={8}>
                  {/* Buttons & Code Section */}
                  <ButtonsCodeSection
                    isOpen={openSections.buttons ?? true}
                    onToggle={() => toggleSection("buttons")}
                  />

                  {/* Badges & Avatars Section */}
                  <BadgesAvatarsSection
                    isOpen={openSections.badges ?? true}
                    onToggle={() => toggleSection("badges")}
                  />

                  {/* Forms & Inputs Section */}
                  <FormsInputsSection
                    formState={formState}
                    isOpen={openSections.forms ?? true}
                    onToggle={() => toggleSection("forms")}
                    updateFormState={updateFormState}
                  />

                  {/* Progress & Task States Section */}
                  <ProgressTaskStatesSection
                    isProgressOpen={openSections.progress ?? true}
                    isStatesOpen={openSections.states ?? true}
                    onProgressToggle={() => toggleSection("progress")}
                    onStatesToggle={() => toggleSection("states")}
                    progressValue={formState.progress}
                    setProgressValue={(value: number) => updateFormState({ progress: value })}
                  />

                  {/* Graph Components Section */}
                  <GraphSection
                    isOpen={openSections.graphs ?? true}
                    onToggle={() => toggleSection("graphs")}
                  />

                  {/* Charts & Gantt Section */}
                  <ChartsGanttSection
                    isOpen={openSections.charts ?? true}
                    onToggle={() => toggleSection("charts")}
                  />
                </VStack>
              </Box>
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
