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
import { Card, HStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiCalendar, FiChevronLeft, FiChevronRight } from "react-icons/fi";

import { RouterLink } from "src/system-components";

export const TimeScheduleCard = () => {
  const { i18n, t: translate } = useTranslation();

  return (
    <Card.Root
      aria-labelledby="time-schedule-card-title"
      asChild
      bg="transparent"
      borderColor="border.subtle"
      size="sm"
      variant="outline"
      width={{ base: "full", md: "auto" }}
    >
      <RouterLink _hover={{ textDecoration: "none" }} color="fg" to="../time-schedule">
        <Card.Body gap={1} px={4} py={3}>
          <HStack gap={2}>
            <FiCalendar />
            <Card.Title fontSize="sm" id="time-schedule-card-title">
              {translate("timeSchedule.title")}
            </Card.Title>
            {i18n.dir() === "rtl" ? <FiChevronLeft /> : <FiChevronRight />}
          </HStack>
          <Card.Description color="fg.muted" fontSize="sm">
            {translate("timeSchedule.description")}
          </Card.Description>
        </Card.Body>
      </RouterLink>
    </Card.Root>
  );
};
