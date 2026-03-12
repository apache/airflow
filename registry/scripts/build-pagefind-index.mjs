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

import fs from 'fs';
import * as pagefind from "pagefind";

async function buildPagefindIndex() {
  console.log('Building PageFind index with custom records...');

  const providers = JSON.parse(fs.readFileSync('src/_data/providers.json', 'utf-8'));
  const modules = JSON.parse(fs.readFileSync('src/_data/modules.json', 'utf-8'));

  // URLs are stored WITHOUT the path prefix. Pagefind derives its basePath
  // from its own import URL (e.g. /registry/pagefind/pagefind.js → /registry/)
  // and prepends it to all result URLs automatically.
  const { index } = await pagefind.createIndex({});

  const providerVersionMap = {};
  for (const provider of providers.providers) {
    providerVersionMap[provider.id] = provider.version;
  }

  let providersAdded = 0;
  let modulesAdded = 0;

  for (const provider of providers.providers) {
    await index.addCustomRecord({
      url: `/providers/${provider.id}/${provider.version}/`,
      content: `${provider.name} ${provider.description}`,
      language: 'en',
      meta: {
        type: 'provider',
        name: provider.name,
        description: provider.description,
        providerId: provider.id,
        providerName: provider.name
      },
      filters: {
        type: ['provider']
      }
    });
    providersAdded++;
  }

  for (const module of modules.modules) {
    const version = providerVersionMap[module.provider_id] || '';
    const url = version
      ? `/providers/${module.provider_id}/${version}/#${module.id}`
      : `/providers/${module.provider_id}/#${module.id}`;

    const content = `${module.name} ${module.name} ${module.name} ${module.short_description} ${module.provider_name} ${module.import_path}`;

    await index.addCustomRecord({
      url: url,
      content: content,
      language: 'en',
      meta: {
        type: 'module',
        className: module.name,
        name: module.name,
        description: module.short_description,
        moduleType: module.type,
        importPath: module.import_path,
        modulePath: module.module_path,
        providerName: module.provider_name,
        providerId: module.provider_id
      },
      filters: {
        type: ['module'],
        moduleType: [module.type],
        providerId: [module.provider_id]
      }
    });
    modulesAdded++;
  }

  await index.writeFiles({
    outputPath: './_site/pagefind'
  });

  await index.deleteIndex();

  // clean up once complete
  await pagefind.close();

  console.log(`✓ Built PageFind index with ${providersAdded + modulesAdded} custom records`);
  console.log(`  - ${providersAdded} providers`);
  console.log(`  - ${modulesAdded} modules`);
}

buildPagefindIndex().catch(err => { console.error(err); process.exit(1); });
