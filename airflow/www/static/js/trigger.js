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

let jsonForm;
const objectFields = new Map();

/**
 * Update the generated JSON DagRun.conf JSON field if any field changed
 */
function updateJSONconf() {
  const jsonStart = document.getElementById('json_start').value;
  const params = JSON.parse(jsonStart);
  const elements = document.getElementById('trigger_form');
  for (let i = 0; i < elements.length; i += 1) {
    if (elements[i].name && elements[i].name.startsWith('element_')) {
      const keyName = elements[i].name.substr(8);
      if (elements[i].type === 'checkbox') {
        params[keyName] = elements[i].checked;
      } else if (elements[i].attributes.valuetype && elements[i].attributes.valuetype.value === 'array') {
        const lines = elements[i].value.split('\n');
        const values = [];
        for (let j = 0; j < lines.length; j += 1) {
          if (lines[j].trim().length > 0) {
            values[values.length] = lines[j].trim();
          }
        }
        params[keyName] = values;
      } else if (elements[i].value.length === 0) {
        params[keyName] = null;
      } else if (elements[i].attributes.valuetype && elements[i].attributes.valuetype.value === 'object') {
        try {
          const textValue = objectFields.get(elements[i].name).getValue();
          if (textValue.length > 0) {
            const objValue = JSON.parse(textValue);
            params[keyName] = objValue;
            objectFields.get(elements[i].name).setValue(JSON.stringify(objValue, null, 4));
          } else {
            params[keyName] = null;
          }
        } catch (e) {
          // ignore JSON parsing errors
        }
      } else if (Number.isNaN(elements[i].value)) {
        params[keyName] = elements[i].value;
      } else if (elements[i].attributes.valuetype && elements[i].attributes.valuetype.value === 'number') {
        params[keyName] = Number(elements[i].value);
      } else {
        params[keyName] = elements[i].value;
      }
    }
  }
  jsonForm.setValue(JSON.stringify(params, null, 4));
}

/**
 * Initialize the form during load of the web page
 */
function initForm() {
  const formSectionsElement = document.getElementById('form_sections');
  const formWithFields = (formSectionsElement != null);

  // Initialize the Generated JSON form or JSON entry form
  const minHeight = 300;
  const maxHeight = (formWithFields ? window.innerHeight / 2 : window.innerHeight) - 450;
  const height = maxHeight > minHeight ? maxHeight : minHeight;
  jsonForm = CodeMirror.fromTextArea(document.getElementById('json'), {
    lineNumbers: true,
    mode: { name: 'javascript', json: true },
    gutters: ['CodeMirror-lint-markers'],
    lint: true,
  });
  jsonForm.setSize(null, height);

  if (formWithFields) {
    // Apply JSON formatting and linting to all object fields in the form
    const elements = document.getElementById('trigger_form');
    for (let i = 0; i < elements.length; i += 1) {
      if (elements[i].name && elements[i].name.startsWith('element_')) {
        if (elements[i].attributes.valuetype
            && elements[i].attributes.valuetype.value === 'object') {
          const field = CodeMirror.fromTextArea(elements[i], {
            lineNumbers: true,
            mode: { name: 'javascript', json: true },
            gutters: ['CodeMirror-lint-markers'],
            lint: true,
          });
          /* eslint-disable no-unused-vars */
          field.on('blur', (cm, change) => { updateJSONconf(); });
          /* eslint-enable no-unused-vars */
          objectFields.set(elements[i].name, field);
        } else if (elements[i].type === 'checkbox') {
          elements[i].addEventListener('change', () => {
            updateJSONconf();
          });
        } else {
          elements[i].addEventListener('blur', () => {
            updateJSONconf();
          });
        }
      }
    }

    // Refreshes JSON entry box sizes when they toggle display
    const formSections = formSectionsElement.value.split(',');
    for (let i = 0; i < formSections.length; i += 1) {
      const toggleBlock = document.getElementById(`${formSections[i]}_toggle`);
      if (toggleBlock) {
        toggleBlock.addEventListener('click', () => {
          setTimeout(() => {
            objectFields.forEach((cm) => {
              cm.refresh();
            });
          }, 300);
        });
      }
    }

    // Validate JSON entry fields before submission
    elements.addEventListener('submit', (event) => {
      updateJSONconf();
      objectFields.forEach((cm) => {
        const textValue = cm.getValue();
        try {
          if (textValue.trim().length > 0) {
            JSON.parse(textValue);
          }
        } catch (ex) {
          window.alert(`Invalid JSON entered, please correct:\n\n${textValue}`);
          cm.focus();
          event.preventDefault();
        }
      });
    });

    // Ensure layout is refreshed on generated JSON as well
    document.getElementById('generated_json_toggle').addEventListener('click', () => {
      setTimeout(() => {
        jsonForm.refresh();
      }, 300);
    });

    // Update generated conf once
    setTimeout(() => {
      updateJSONconf();
    }, 100);
  }
}
initForm();

window.updateJSONconf = updateJSONconf;

function setRecentConfig(e) {
  let { value } = e.target;
  try {
    const json = JSON.parse(value);
    value = JSON.stringify(json, null, 2);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('config is not valid JSON format');
  }

  // TODO Values need to be applied to all form fields accordingly!
  document.querySelector('.CodeMirror').CodeMirror.setValue(value);
}

recentConfigList.addEventListener('change', setRecentConfig);
