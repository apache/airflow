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

/*
 * Sections of the docker-stack docs moved to other pages or were renamed when the docs were split
 * into separate pages. Server-side redirects cannot see the "#anchor" part of a URL, so links to the
 * old sections are redirected here: when the anchor of the URL does not exist on the page, but is
 * listed below for that page, the browser goes to the new location instead.
 */
(function () {
  "use strict";

  var MOVED = {
    "index.html": {
      "fixing-images-at-release-time": "security-release-time.html",
      "fixing-image-at-release-time": "security-release-time.html#fixing-image-at-release-time",
      "what-should-i-do-if-my-security-scan-shows-critical-and-high-vulnerabilities-in-the-image":
        "security-scan-results.html",
      "support": "index.html#supported-databases",
      "usage": "entrypoint.html#folders-and-defaults",
    },
    "build.html": {
      "build-build-image": "why-build-your-own-image.html#build-build-image",
      "building-the-image": "why-build-your-own-image.html",
      "quick-start-scenarios-of-image-extending": "build.html#build-small-production-image",
      "building-images-primer": "build.html#extending-vs-customizing-the-image",
      "base-images": "image-variants.html",
      "important-notes-for-the-base-images": "image-variants.html",
      "extending-the-image": "build-extending.html",
      "adding-new-apt-package": "build-extending.html#apt-packages",
      "adding-new-pypi-packages-individually": "build-extending.html#pypi-packages",
      "adding-packages-from-requirements-txt": "build-extending.html#dependencies-in-requirements-txt",
      "embedding-dags": "build-extending.html#embedding-dags",
      "add-airflow-configuration-with-environment-variables": "build-extending.html#airflow-configuration",
      "examples-of-image-extending": "build-extending.html#more-extending-examples",
      "example-of-setting-own-airflow-providers": "build-extending.html#provider-versions",
      "example-of-adding-airflow-provider-package-and-apt-package":
        "build-extending.html#provider-with-apt-package",
      "example-of-adding-apt-package": "build-extending.html#apt-packages",
      "example-of-adding-pypi-package": "build-extending.html#pypi-packages",
      "example-of-adding-pypi-package-with-constraints": "build-extending.html#pypi-with-constraints",
      "example-of-adding-pypi-package-with-uv": "build-extending.html#pypi-with-uv",
      "example-of-adding-packages-from-requirements-txt":
        "build-extending.html#dependencies-in-requirements-txt",
      "example-of-adding-a-python-venv-for-externalpythonoperator":
        "build-extending.html#python-venv-for-external-tasks",
      "example-when-writable-directory-is-needed": "build-extending.html#writable-directory",
      "example-when-you-add-packages-requiring-compilation":
        "build-extending.html#packages-requiring-compilation",
      "example-when-you-want-to-embed-dags": "build-extending.html#embedding-dags",
      "example-of-changing-airflow-configuration-using-environment-variables":
        "build-extending.html#airflow-configuration",
      "why-customizing-the-image": "build-customizing.html",
      "customizing-the-image": "build-customizing.html",
      "using-docker-context-files": "build-customizing.html#using-docker-context-files",
      "examples-of-image-customizing": "build-customizing.html#building-from-pypi-packages",
      "image-build-pypi": "build-customizing.html#image-build-pypi",
      "building-from-pypi-packages": "build-customizing.html#building-from-pypi-packages",
      "image-build-optimized": "build-customizing.html#image-build-optimized",
      "building-optimized-images": "build-customizing.html#building-optimized-images",
      "image-build-uv": "build-customizing.html#image-build-uv",
      "building-prod-images-using-uv-as-the-package-installer":
        "build-customizing.html#building-prod-images-using-uv-as-the-package-installer",
      "image-build-mysql": "build-customizing.html#image-build-mysql",
      "building-images-with-mysql-client": "build-customizing.html#building-images-with-mysql-client",
      "image-build-custom": "build-customizing.html#image-build-custom",
      "using-custom-installation-sources": "build-customizing.html#using-custom-installation-sources",
      "image-build-secure-environments": "build-advanced.html#image-build-secure-environments",
      "build-images-in-security-restricted-environments":
        "build-advanced.html#build-images-in-security-restricted-environments",
      "image-build-fips": "build-advanced.html#image-build-fips",
      "build-images-in-fips-compliant-environments":
        "build-advanced.html#build-images-in-fips-compliant-environments",
      "image-hardened-base": "build-advanced.html#image-hardened-base",
      "properties-of-the-hardened-base-image": "build-advanced.html#properties-of-the-hardened-base-image",
      "image-legacy-flavor": "legacy-images.html",
      "debian-trixie-and-the-legacy-image-flavor": "build-advanced.html#debian-trixie",
      "modifying-the-dockerfile": "build-advanced.html#modifying-the-dockerfile",
      "more-details": "build-advanced.html#more-details",
      "build-args-reference": "build-arg-ref.html",
      "the-architecture-of-the-images": "build-advanced.html#more-details",
      "dependencies-caching": "build-advanced.html#dependency-caching",
    },
    "entrypoint.html": {
      "entrypoint": "entrypoint.html",
      "allowing-arbitrary-user-to-run-the-container": "entrypoint.html#arbitrary-user",
      "waits-for-airflow-db-connection": "entrypoint.html#waiting-for-the-database",
      "waits-for-celery-broker-connection": "entrypoint.html#waiting-for-the-celery-broker",
      "execute-custom-code-before-the-airflow-entrypoint": "entrypoint.html#custom-code-before-the-entrypoint",
      "adding-custom-image-behaviour": "entrypoint.html#custom-image-behaviour",
      "additional-quick-test-options": "testing.html",
      "upgrading-airflow-db": "testing.html#upgrading-airflow-db",
      "creating-admin-user": "testing.html#creating-admin-user",
      "installing-additional-requirements": "testing.html#installing-additional-requirements",
    },
  };

  function redirect() {
    var anchor = decodeURIComponent(window.location.hash.slice(1));
    if (!anchor || document.getElementById(anchor)) {
      return;
    }
    var page = window.location.pathname.split("/").pop() || "index.html";
    var target = (MOVED[page] || {})[anchor];
    if (target) {
      window.location.replace(target);
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", redirect);
  } else {
    redirect();
  }
  window.addEventListener("hashchange", redirect);
})();
