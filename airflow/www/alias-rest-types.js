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

const ts = require('typescript');
const fs = require('fs');

/* This library does three things to make openapi-typescript generation easier to use.
 * 1. Creates capitalized exports for Paths and Operations
 * 2. Alias Variables based either on the Path name or the Operation ID, such as:
 *      export type ListProjectsVariables = operations['listProjects']['parameters']['query'];
 * 3. Aliases the returned data types, such as:
 *      export type ConnectionArgument = components['schemas']['ConnectionArgument'];
 */

/* Finds all words, capitalizes them, and removes all other characters. */
const toPascalCase = (str) => (
  (str.match(/[a-zA-Z0-9]+/g) || [])
    .map((w) => `${w.charAt(0).toUpperCase()}${w.slice(1)}`)
    .join('')
);

/* Adds a prefix to a type prop as necessary.
 * ('', 'components') => 'components'
 */
const prefixPath = (rootPrefix, prop) => (rootPrefix ? `${rootPrefix}['${prop}']` : prop);

// Recursively find child nodes by name.
const findNode = (node, ...names) => {
  if (!node || names.length === 0) return node;

  const children = node.members || node.type.members;

  if (!children) {
    return undefined;
  }

  const child = children.find((c) => c.name?.text === names[0]);

  return findNode(child, ...names.slice(1));
};

// Generate Variable Type Aliases for a given path or operation
const generateVariableAliases = (node, operationPath, operationName) => {
  const variableTypes = [];
  const hasPath = !!findNode(node, 'parameters', 'path');
  const hasQuery = !!findNode(node, 'parameters', 'query');
  const hasBody = !!findNode(node, 'requestBody', 'content', 'application/json');

  if (hasPath) variableTypes.push(`${operationPath}['parameters']['path']`);
  if (hasQuery) variableTypes.push(`${operationPath}['parameters']['query']`);
  if (hasBody) variableTypes.push(`${operationPath}['requestBody']['content']['application/json']`);

  if (variableTypes.length === 0) return '';
  const typeName = `${toPascalCase(operationName)}Variables`;
  return [typeName, `export type ${typeName} = ${variableTypes.join(' & ')};`];
};

// Generate Type Aliases
const generateAliases = (rootNode, writeText, prefix = '') => {
  // Loop through the root AST nodes of the file
  ts.forEachChild(rootNode, (node) => {
    // Response Data Types
    if (ts.isInterfaceDeclaration(node) && node.name?.text === 'components') {
      const schemaMemberNames = findNode(node, 'schemas').type.members.map((n) => n.name?.text);

      const types = schemaMemberNames.map((n) => [
        `${n}`,
        `export type ${n} = CamelCasedPropertiesDeep<${prefixPath(prefix, 'components')}['schemas']['${n}']>;`,
      ]);
      if (types.length) {
        writeText.push(['comment', `Types for returned data ${prefix}`]);
        writeText.push(...types);
      }
    }

    // Paths referencing an operation are skipped
    if (node.name?.text === 'paths') {
      if (!prefix) {
        writeText.push(['comment', 'Alias paths to PascalCase.']);
        writeText.push(['Paths', 'export type Paths = paths;']);
      }

      const types = [];

      (node.members || node.type.members).forEach((path) => {
        const methodNames = path.type.members.map((m) => m.name.text);
        const methodTypes = methodNames.map((m) => (
          generateVariableAliases(
            findNode(path, m),
            `${prefixPath(prefix, 'paths')}['${path.name?.text}']['${m}']`,
            `${path.name.text}${toPascalCase(m)}`,
          )));
        types.push(...methodTypes.filter((m) => !!m));
      });

      if (types.length) {
        writeText.push(['comment', `Types for path operation variables ${prefix}`]);
        writeText.push(...types);
      }
    }

    // operationIds are defined
    if (node.name?.text === 'operations') {
      if (!prefix) {
        writeText.push(['comment', 'Alias operations to PascalCase.']);
        writeText.push(['Operations', 'export type Operations = operations;']);
      }

      const types = (node.members || node.type.members).map((operation) => (
        generateVariableAliases(
          operation,
          `${prefixPath(prefix, 'operations')}['${operation.name.text}']`,
          operation.name.text,
        )));
      if (types.length) {
        writeText.push(['comment', `Types for operation variables ${prefix}`]);
        writeText.push(...types);
        writeText.push('\n');
      }
    }

    // recursively call this for any externals
    if (ts.isInterfaceDeclaration(node) && node.name?.text === 'external') {
      node.members.forEach((external) => {
        generateAliases(external.type, writeText, `external['${external.name.text}']`);
      });
    }
  });
};

const license = `/*!
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
*/`;

function generate(file) {
  // Create a Program to represent the project, then pull out the
  // source file to parse its AST.
  const program = ts.createProgram([file], { allowJs: true });
  const sourceFile = program.getSourceFile(file);
  const writeText = [];
  writeText.push(['block', license]);
  writeText.push(['comment', 'eslint-disable']);
  // eslint-disable-next-line quotes
  writeText.push(['block', `import type { CamelCasedPropertiesDeep } from 'type-fest';`]);
  writeText.push(['block', sourceFile.text]);
  generateAliases(sourceFile, writeText);

  const finalText = writeText
    // Deduplicate types
    .map((pair) => {
      // keep all comments and code blocks
      if (pair[0] === 'comment' || pair[0] === 'block') return pair;
      // return the first instance of this key only
      const firstInstance = writeText.find((p) => p[0] === pair[0]);
      return firstInstance === pair ? pair : ['comment', `Duplicate removed: ${pair[1]}`];
    })
    // Remove undefined created above
    .filter((p) => !!p)
    // Escape comments and flatten.
    .map((pair) => (pair[0] === 'comment' ? `\n/* ${pair[1]} */` : pair[1]))
    .join('\n');

  fs.writeFileSync(file, finalText, (err) => {
    if (err) {
      console.error(err);
    }
  });
}

generate(process.argv[2]);
