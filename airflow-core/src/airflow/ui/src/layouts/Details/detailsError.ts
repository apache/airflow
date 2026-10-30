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
import { createContext, useContext, useEffect } from "react";

const ignoreError = () => undefined;

/**
 * Sink for the load error of whichever detail level is open.
 *
 * The status chip that shows it sits in the layout header, above the outlet the levels render
 * into, so the error travels up rather than down. `DagDetailsRoute` provides the setter and holds
 * the value; outside it the error is nobody's to show, so the default drops it.
 */
export const SetDetailsErrorContext = createContext<(error: unknown) => void>(ignoreError);

/** Hand the level's load error to the ``DetailsLayout`` above, for the Dag status chip. */
export const useReportDetailsError = (error: unknown) => {
  const setDetailsError = useContext(SetDetailsErrorContext);

  useEffect(() => {
    setDetailsError(error);

    return () => setDetailsError(undefined);
  }, [error, setDetailsError]);
};
