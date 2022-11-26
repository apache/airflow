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

/* global document, CodeMirror, window */

const GRID_DATA_URL = '/object/grid_data';
const textArea = document.getElementById('json');
const dagId = document.getElementById('dag-id').getAttribute('data-dag-id');
const recentConfigList = document.getElementById('recent_configs');
const minHeight = 300;
const maxHeight = window.innerHeight - 450;
const height = maxHeight > minHeight ? maxHeight : minHeight;

CodeMirror.fromTextArea(textArea, {
  lineNumbers: true,
  mode: {
    name: 'javascript',
    json: true,
  },
  gutters: ['CodeMirror-lint-markers'],
  lint: true,
})
  .setSize(null, height);

async function loadRecentConfigs() {
  try {
    const response = await fetch(`${GRID_DATA_URL}?dag_id=${dagId}&num_runs=10`);
    const resJson = await response.json();

    const seenConfigs = new Set();
    const configsToLoad = resJson.dag_runs.map((run) => (run.conf)).filter((conf) => {
      if (!conf || seenConfigs.has(conf)) return false;
      seenConfigs.add(conf);
      return true;
    });

    configsToLoad.forEach((config) => {
      const opt = document.createElement('option');
      opt.value = config;
      opt.innerHTML = config.replaceAll('"', '&quot;');
      recentConfigList.appendChild(opt);
    });
  } catch (e) {
    // Continue loading the page without recent configs
  }
}

function setRecentConfig(e) {
  document.querySelector('.CodeMirror').CodeMirror.setValue(e.target.value);
}

recentConfigList.addEventListener('change', setRecentConfig);
loadRecentConfigs();
