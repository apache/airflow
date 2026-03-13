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

// Connection Builder - chip grid with shared expansion panel
(function () {
  var dataEl = document.getElementById("connections-data");
  if (!dataEl) return;

  var connectionTypes;
  try {
    connectionTypes = JSON.parse(dataEl.textContent || "[]");
  } catch (e) {
    return;
  }
  if (!connectionTypes || !connectionTypes.length) return;

  // Index connection data by connection_type
  var connDataMap = {};
  connectionTypes.forEach(function (ct) {
    connDataMap[ct.connection_type] = ct;
  });

  var panel = document.getElementById("conn-builder-panel");
  var formContainer = document.getElementById("conn-builder-form");
  var titleEl = document.getElementById("conn-builder-title");
  var docsLink = document.getElementById("conn-builder-docs");
  var closeBtn = document.getElementById("conn-builder-close");
  var chips = document.querySelectorAll(".conn-chip");

  if (!panel || !formContainer || !titleEl) return;

  var activeConnType = null;
  var renderedForms = {};  // Cache rendered HTML per conn type
  var debounceTimers = {};

  // Chip click: select/deselect connection type
  chips.forEach(function (chip) {
    chip.addEventListener("click", function () {
      var connType = chip.dataset.connType;

      // Toggle off if already active
      if (activeConnType === connType) {
        closePanel();
        return;
      }

      // Deactivate previous chip
      chips.forEach(function (c) { c.classList.remove("active"); });
      chip.classList.add("active");
      activeConnType = connType;

      // Set title
      titleEl.textContent = connType;

      // Show/hide docs link (URL is resolved per-connection-type by extract scripts)
      if (docsLink) {
        var docsUrl = chip.dataset.docsUrl;
        if (docsUrl) {
          docsLink.href = docsUrl;
          docsLink.hidden = false;
        } else {
          docsLink.hidden = true;
        }
      }

      // Render form (or restore cached)
      var data = connDataMap[connType];
      if (!data) return;

      if (renderedForms[connType]) {
        formContainer.innerHTML = renderedForms[connType];
        attachFormListeners(formContainer, connType, data);
      } else {
        renderConnectionForm(formContainer, connType, data);
        renderedForms[connType] = formContainer.innerHTML;
      }

      // Show panel
      panel.hidden = false;
    });
  });

  // Close button
  if (closeBtn) {
    closeBtn.addEventListener("click", closePanel);
  }

  function closePanel() {
    // Save current form state before closing
    if (activeConnType) {
      renderedForms[activeConnType] = formContainer.innerHTML;
    }
    chips.forEach(function (c) { c.classList.remove("active"); });
    panel.hidden = true;
    activeConnType = null;
  }

  function renderConnectionForm(container, connType, data) {
    var connIdDefault = connType.replace(/[^a-zA-Z0-9]/g, "_");
    var html = '<div class="conn-field">';
    html += '<label for="conn-id-' + connType + '">Connection ID</label>';
    html +=
      '<input type="text" id="conn-id-' + connType +
      '" data-field="conn_id" value="' + escapeAttr(connIdDefault) +
      '" placeholder="my_conn_id">';
    html += "</div>";

    // Standard fields
    var standardOrder = ["host", "port", "login", "password", "schema", "extra", "description"];
    var stdFields = data.standard_fields || {};
    standardOrder.forEach(function (key) {
      var field = stdFields[key];
      if (!field || !field.visible) return;
      html += renderField(connType, key, field, false);
    });

    // Custom fields
    var customFields = data.custom_fields || {};
    Object.keys(customFields).forEach(function (key) {
      html += renderField(connType, key, customFields[key], true);
    });

    // Export panel
    html += renderExportPanel(connType);

    container.innerHTML = html;
    attachFormListeners(container, connType, data);
    updateExports(connType, container, data);
  }

  function attachFormListeners(container, connType, data) {
    // Input listeners for live export updates
    container.querySelectorAll("input, textarea, select").forEach(function (input) {
      input.addEventListener("input", function () {
        debouncedUpdate(connType, container, data);
      });
      input.addEventListener("change", function () {
        debouncedUpdate(connType, container, data);
      });
    });

    // Tab switching
    container.querySelectorAll(".conn-export-tab").forEach(function (tab) {
      tab.addEventListener("click", function () {
        container.querySelectorAll(".conn-export-tab").forEach(function (t) {
          t.classList.remove("active");
        });
        tab.classList.add("active");
        var format = tab.dataset.format;
        container.querySelectorAll(".conn-export-content").forEach(function (c) {
          c.hidden = c.dataset.format !== format;
        });
      });
    });

    // Copy buttons
    container.querySelectorAll(".conn-copy-btn").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var targetId = btn.dataset.copyTarget;
        var target = document.getElementById(targetId);
        if (!target || !navigator.clipboard) return;
        navigator.clipboard.writeText(target.textContent || "").then(
          function () {
            var origHTML = btn.innerHTML;
            btn.innerHTML =
              '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" style="color:var(--color-green-400)"></path></svg>';
            setTimeout(function () { btn.innerHTML = origHTML; }, 2000);
          },
          function () {}
        );
      });
    });

    // Update exports on attach (handles restored forms)
    updateExports(connType, container, data);
  }

  function renderField(connType, key, field, isCustom) {
    var label = field.label || key;
    var id = "conn-" + connType + "-" + key;
    var isSensitive =
      key === "password" || field.is_sensitive || field.format === "password";
    var fieldType = field.type || "string";
    var html = '<div class="conn-field">';
    html += '<label for="' + id + '">' + escapeHTML(label);
    if (isSensitive) {
      html += ' <span class="field-sensitive">sensitive</span>';
    }
    html += "</label>";

    if (field.enum && field.enum.length) {
      html += '<select id="' + id + '" data-field="' + key + '"';
      if (isCustom) html += ' data-custom="true"';
      html += ">";
      html += '<option value="">-- Select --</option>';
      field.enum.forEach(function (opt) {
        var selected = field.default === opt ? " selected" : "";
        html += '<option value="' + escapeAttr(opt) + '"' + selected + ">" + escapeHTML(opt) + "</option>";
      });
      html += "</select>";
    } else if (fieldType === "boolean") {
      var checked = field.default === true || field.default === "true" ? " checked" : "";
      html += '<label class="conn-checkbox"><input type="checkbox" id="' + id + '" data-field="' + key + '"';
      if (isCustom) html += ' data-custom="true"';
      html += ' data-type="boolean"' + checked + ">";
      html += " " + escapeHTML(label) + "</label>";
    } else if (key === "extra" || fieldType === "object") {
      html += '<textarea id="' + id + '" data-field="' + key + '" rows="3"';
      if (isCustom) html += ' data-custom="true"';
      if (field.placeholder) html += ' placeholder="' + escapeAttr(field.placeholder) + '"';
      html += "></textarea>";
    } else if (fieldType === "integer" || fieldType === "number") {
      html += '<input type="number" id="' + id + '" data-field="' + key + '"';
      if (isCustom) html += ' data-custom="true"';
      if (field.default != null) html += ' value="' + escapeAttr(String(field.default)) + '"';
      if (field.minimum != null) html += ' min="' + escapeAttr(String(field.minimum)) + '"';
      if (field.placeholder) html += ' placeholder="' + escapeAttr(field.placeholder) + '"';
      html += ">";
    } else {
      var inputType = isSensitive ? "password" : "text";
      html += '<input type="' + inputType + '" id="' + id + '" data-field="' + key + '"';
      if (isCustom) html += ' data-custom="true"';
      if (field.default != null && fieldType !== "boolean") html += ' value="' + escapeAttr(String(field.default)) + '"';
      if (field.placeholder) html += ' placeholder="' + escapeAttr(field.placeholder) + '"';
      html += ">";
    }

    if (field.description) {
      html += '<span class="field-help">' + escapeHTML(field.description) + "</span>";
    }
    html += "</div>";
    return html;
  }

  function renderExportPanel(connType) {
    var uid = connType.replace(/[^a-zA-Z0-9]/g, "_");
    var html = '<div class="conn-export">';
    html += '<div class="conn-export-tabs">';
    html += '<button class="conn-export-tab active" data-format="uri" type="button">URI</button>';
    html += '<button class="conn-export-tab" data-format="json" type="button">JSON</button>';
    html += '<button class="conn-export-tab" data-format="env" type="button">Env Var</button>';
    html += "</div>";

    ["uri", "json", "env"].forEach(function (fmt) {
      var hidden = fmt !== "uri" ? " hidden" : "";
      html += '<div class="conn-export-content" data-format="' + fmt + '"' + hidden + ">";
      html += '<div class="conn-export-output">';
      html += '<pre id="conn-export-' + uid + "-" + fmt + '"></pre>';
      html += '<button class="conn-copy-btn" data-copy-target="conn-export-' + uid + "-" + fmt + '" type="button" title="Copy">';
      html += '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" /></svg>';
      html += "</button></div></div>";
    });

    html += "</div>";
    return html;
  }

  function debouncedUpdate(connType, container, data) {
    clearTimeout(debounceTimers[connType]);
    debounceTimers[connType] = setTimeout(function () {
      updateExports(connType, container, data);
    }, 100);
  }

  function collectFieldValues(container) {
    var values = { standard: {}, custom: {}, conn_id: "" };
    container.querySelectorAll("[data-field]").forEach(function (el) {
      var key = el.dataset.field;
      var val = el.type === "checkbox" ? el.checked : el.value;
      if (key === "conn_id") {
        values.conn_id = val;
      } else if (el.dataset.custom === "true") {
        values.custom[key] = val;
      } else {
        values.standard[key] = val;
      }
    });
    return values;
  }

  function updateExports(connType, container, data) {
    var values = collectFieldValues(container);
    var uid = connType.replace(/[^a-zA-Z0-9]/g, "_");

    var uriEl = document.getElementById("conn-export-" + uid + "-uri");
    var jsonEl = document.getElementById("conn-export-" + uid + "-json");
    var envEl = document.getElementById("conn-export-" + uid + "-env");

    if (uriEl) uriEl.textContent = generateURI(connType, values);
    if (jsonEl) jsonEl.textContent = generateJSON(connType, values);
    if (envEl) envEl.textContent = generateEnvVar(connType, values);
  }

  function generateURI(connType, values) {
    var s = values.standard;
    var login = s.login || "";
    var password = s.password || "";
    var host = s.host || "";
    var port = s.port || "";
    var schema = s.schema || "";
    var extra = s.extra || "";

    var extraObj = {};
    if (extra) {
      try { extraObj = JSON.parse(extra); }
      catch (e) { extraObj = null; }
    }

    var customEntries = Object.keys(values.custom).filter(function (k) {
      var v = values.custom[k];
      return v !== "" && v !== false && v != null;
    });

    var uri = connType + "://";

    if (login || password) {
      if (login) uri += encodeURIComponent(login);
      if (password) uri += ":" + encodeURIComponent(password);
      uri += "@";
    }

    if (host) {
      if (host.indexOf(':') !== -1) uri += '[' + host + ']';
      else uri += encodeURIComponent(host);
    }
    if (port) uri += ":" + encodeURIComponent(port);

    if (schema) uri += "/" + encodeURIComponent(schema);
    else if (extra || customEntries.length) uri += "/";

    var params = [];
    if (extraObj && typeof extraObj === "object") {
      Object.keys(extraObj).forEach(function (k) {
        var v = extraObj[k];
        if (typeof v === "object") {
          params.push(encodeURIComponent(k) + "=" + encodeURIComponent(JSON.stringify(v)));
        } else {
          params.push(encodeURIComponent(k) + "=" + encodeURIComponent(String(v)));
        }
      });
    } else if (extra) {
      params.push("__extra__=" + encodeURIComponent(extra));
    }

    customEntries.forEach(function (k) {
      params.push(encodeURIComponent(k) + "=" + encodeURIComponent(String(values.custom[k])));
    });

    if (params.length) uri += "?" + params.join("&");
    return uri;
  }

  function generateJSON(connType, values) {
    var obj = { conn_type: connType };
    var s = values.standard;

    if (s.host) obj.host = s.host;
    if (s.port) obj.port = isNaN(Number(s.port)) ? s.port : Number(s.port);
    if (s.login) obj.login = s.login;
    if (s.password) obj.password = s.password;
    if (s.schema) obj.schema = s.schema;
    if (s.description) obj.description = s.description;

    var extraObj = {};
    var hasExtra = false;
    if (s.extra) {
      try { extraObj = JSON.parse(s.extra); hasExtra = true; }
      catch (e) { obj.extra = s.extra; hasExtra = true; }
    }

    Object.keys(values.custom).forEach(function (k) {
      var v = values.custom[k];
      if (v === "" || v == null) return;
      if (v === false) { extraObj[k] = false; }
      else if (v === true) { extraObj[k] = true; }
      else if (!isNaN(Number(v)) && v !== "") { extraObj[k] = Number(v); }
      else { extraObj[k] = v; }
      hasExtra = true;
    });

    if (hasExtra && typeof obj.extra !== "string") {
      if (Object.keys(extraObj).length) obj.extra = extraObj;
    }

    return JSON.stringify(obj, null, 2);
  }

  function generateEnvVar(connType, values) {
    var connId = values.conn_id || connType.toUpperCase().replace(/[^A-Z0-9]/g, "_");
    var jsonStr = generateJSON(connType, values);
    var escaped = jsonStr.replace(/'/g, "'\\''");
    return "AIRFLOW_CONN_" + connId.toUpperCase().replace(/[^A-Z0-9]/g, "_") + "='" + escaped + "'";
  }

  function escapeHTML(str) {
    if (!str) return "";
    return String(str).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  function escapeAttr(str) {
    if (!str) return "";
    return String(str).replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }
})();
