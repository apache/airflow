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
import { useReactFlow } from "@xyflow/react";
import { useEffect } from "react";

// Fits the viewport whenever a new layout is committed. Must live inside
// <ReactFlow> to call useReactFlow(). Using layoutData as the dep means it
// only fires when ELK produces a new layout, not on task-instance updates or
// selection changes (unlike the `fitView` prop, which runs on every re-mount).
export const FitViewOnLayout = ({ layoutData }: { readonly layoutData: object | undefined }) => {
  const { fitView } = useReactFlow();

  useEffect(() => {
    if (layoutData !== undefined) {
      void fitView({ padding: 0.1 });
    }
  }, [fitView, layoutData]);

  return null;
};
