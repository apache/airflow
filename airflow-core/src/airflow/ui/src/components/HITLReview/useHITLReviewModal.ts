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
import { useEffect, useRef } from "react";
import { useLocation, useNavigate } from "react-router-dom";

export const useHITLReviewRouteModalSync = ({
  onClose,
  onOpen,
}: {
  readonly onClose: () => void;
  readonly onOpen: () => void;
}) => {
  const location = useLocation();
  const navigate = useNavigate();
  const openedFromRouteRef = useRef(false);
  const isHITLReviewRoute = location.pathname.endsWith("/required_actions");

  useEffect(() => {
    if (isHITLReviewRoute) {
      openedFromRouteRef.current = true;
      onOpen();
    } else if (openedFromRouteRef.current) {
      openedFromRouteRef.current = false;
      onClose();
    }
  }, [isHITLReviewRoute, onClose, onOpen]);

  const onCloseHITLReview = () => {
    openedFromRouteRef.current = false;
    onClose();

    if (isHITLReviewRoute) {
      const redirectPath = location.pathname.replace(/\/required_actions$/u, "") || "/";

      void Promise.resolve(navigate(redirectPath, { replace: true }));
    }
  };

  return {
    onCloseHITLReview,
  };
};
