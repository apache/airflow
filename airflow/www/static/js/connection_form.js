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
/**
 * Created by janomar on 23/07/15.
 */

/* global document, DOMParser, $ */

function decode(str) {
  return new DOMParser().parseFromString(str, 'text/html').documentElement.textContent;
}

/**
 * Returns a map of connection type to its controls.
 */
function getConnTypesToControlsMap() {
  const connTypesToControlsMap = new Map();

  const extraFormControls = Array.from(document.querySelectorAll("[id^='extra__'"));
  extraFormControls.forEach((control) => {
    const connTypeEnd = control.id.indexOf('__', 'extra__'.length);
    const connType = control.id.substring('extra__'.length, connTypeEnd);

    const controls = connTypesToControlsMap.has(connType)
      ? connTypesToControlsMap.get(connType)
      : [];

    controls.push(control.parentElement.parentElement);
    connTypesToControlsMap.set(connType, controls);
  });

  return connTypesToControlsMap;
}

/**
 * Returns the DOM element that contains the different controls.
 */
function getControlsContainer() {
  return document.getElementById('conn_id')
    .parentElement
    .parentElement
    .parentElement;
}

/**
   * Restores the behaviour for all fields. Used to restore fields to a
   * well-known state during the change of connection types.
   */
function restoreFieldBehaviours() {
  Array.from(document.querySelectorAll('label[data-origText]')).forEach((elem) => {
    elem.innerText = elem.dataset.origText;
    delete elem.dataset.origText;
  });

  Array.from(document.querySelectorAll('.form-control')).forEach((elem) => {
    elem.placeholder = '';
    elem.parentElement.parentElement.classList.remove('hide');
  });
}

/**
   * Applies special behaviour for fields. The behaviour is defined through
   * config, passed by the server.
   *
   * @param {string} connection The connection object to apply to.
   */
function applyFieldBehaviours(connection) {
  if (connection) {
    if (Array.isArray(connection.hidden_fields)) {
      connection.hidden_fields.forEach((field) => {
        document.getElementById(field)
          .parentElement
          .parentElement
          .classList
          .add('hide');
      });
    }

    if (connection.relabeling) {
      Object.keys(connection.relabeling).forEach((field) => {
        const label = document.querySelector(`label[for='${field}']`);
        label.dataset.origText = label.innerText;
        label.innerText = connection.relabeling[field];
      });
    }

    if (connection.placeholders) {
      Object.keys(connection.placeholders).forEach((field) => {
        const placeholder = connection.placeholders[field];
        document.getElementById(field).placeholder = placeholder;
      });
    }
  }
}

$(document).ready(() => {
  const fieldBehavioursElem = document.getElementById('field_behaviours');
  const config = JSON.parse(decode(fieldBehavioursElem.textContent));

  // Prevent login/password fields from triggering browser auth extensions
  const form = document.getElementById('model_form');
  if (form) form.setAttribute('autocomplete', 'off');

  // Save all DOM elements into a map on load.
  const controlsContainer = getControlsContainer();
  const connTypesToControlsMap = getConnTypesToControlsMap();

  /**
   * Changes the connection type.
   * @param {string} connType The connection type to change to.
   */
  function changeConnType(connType) {
    Array.from(connTypesToControlsMap.values()).forEach((controls) => {
      controls
        .filter((control) => control.parentElement === controlsContainer)
        .forEach((control) => controlsContainer.removeChild(control));
    });

    const controls = connTypesToControlsMap.get(connType) || [];
    controls.forEach((control) => controlsContainer.appendChild(control));

    // Restore field behaviours.
    restoreFieldBehaviours();

    // Apply behaviours to fields.
    applyFieldBehaviours(config[connType]);
  }

  const connTypeElem = document.getElementById('conn_type');
  $(connTypeElem).on('change', (e) => {
    const connType = e.target.value;
    changeConnType(connType);
  });

  // Initialize the form by setting a connection type.
  changeConnType(connTypeElem.value);
});
