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
// Official upload on the SadeVEVO channel, the same video the SmoothOperator task logs.
const VIDEO_ID = "4TYv2PhG89A";

export const WATCH_URL = `https://www.youtube.com/watch?v=${VIDEO_ID}`;
// Embedding the player shares the viewer's IP address and the Airflow site's address with Google;
// the scene tells viewers so before it contacts YouTube, and links the policy and terms.
export const PRIVACY_URL = "https://policies.google.com/privacy";
export const TERMS_URL = "https://www.youtube.com/t/terms";

// A plain embed on YouTube's privacy-enhanced host. That host only stops views from being used to
// personalise ads; it still collects data, the frame keeps storage in its own origin, and with
// autoplay that collection starts without the viewer touching the player. No IFrame Player API
// script: it would run Google's code in Airflow's own origin, and the scene does not need the
// player's state. Like any embed, it tells Google the Airflow origin through the Referer.
export const EMBED_URL = `https://www.youtube-nocookie.com/embed/${VIDEO_ID}?autoplay=1&playsinline=1&rel=0`;
