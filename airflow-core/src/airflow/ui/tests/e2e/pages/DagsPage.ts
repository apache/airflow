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
import type { Locator } from '@playwright/test';

import { BasePage } from './BasePage';

/**
 * DAGs Page Object
 */
export class DagsPage extends BasePage {
  // Core page elements
  readonly dagsTable: Locator;
  readonly triggerButton: Locator;
  readonly confirmButton: Locator;
  readonly stateElement: Locator;

  // Page URLs
  readonly dagsListUrl = '/dags';

  constructor(page: any) {
    super(page);
    this.dagsTable = page.locator('div:has(a[href*="/dags/"])');
    this.triggerButton = page.locator('button[aria-label="Trigger Dag"]:has-text("Trigger")');
    this.confirmButton = page.locator('button:has-text("Trigger")').nth(1);
    this.stateElement = page.locator('*:has-text("State") + *').first();
  }

  // URL builders for dynamic paths
  getDagDetailUrl(dagName: string): string {
    return `/dags/${dagName}`;
  }

  getDagRunDetailsUrl(dagName: string, dagRunId: string): string {
    return `/dags/${dagName}/runs/${dagRunId}/details`;
  }



  /**
   * Navigate to DAGs list page
   */
  async navigate(): Promise<void> {
    await this.navigateTo(this.dagsListUrl);
  }

  /**
   * Navigate to DAG detail page
   */
  async navigateToDagDetail(dagName: string): Promise<void> {
    await this.navigateTo(this.getDagDetailUrl(dagName));
  }

  /**
   * Trigger a DAG run
   */
  async triggerDag(dagName: string): Promise<string | null> {
    await this.navigateToDagDetail(dagName);
    await this.triggerButton.waitFor({ state: 'visible', timeout: 10000 });
    await this.triggerButton.click();
    const dagRunId = await this.handleTriggerDialog();
    return dagRunId;
  }

  private async handleTriggerDialog(): Promise<string | null> {
    await this.page.waitForTimeout(1000);

    const responsePromise = this.page.waitForResponse(
      response => {
        const url = response.url();
        const method = response.request().method();
        return method === 'POST' && url.includes('dagRuns') && !url.includes('hitlDetails');
      },
      { timeout: 10000 }
    ).catch(() => null);

    await this.confirmButton.waitFor({ state: 'visible', timeout: 8000 });
    await this.page.waitForTimeout(2000);
    await this.confirmButton.click({ force: true });

    const apiResponse = await responsePromise;
    if (apiResponse) {
      try {
        const responseBody = await apiResponse.text();
        const responseJson = JSON.parse(responseBody);
        if (responseJson.dag_run_id) {
          return responseJson.dag_run_id;
        }
      } catch (error) {
        // Response parsing failed
      }
    }

    return null;
  }

  async verifyDagRunStatus(dagName: string, dagRunId: string | null): Promise<void> {
    if (!dagRunId) {
      return;
    }

    await this.page.goto(this.getDagRunDetailsUrl(dagName, dagRunId), {
      waitUntil: 'domcontentloaded',
      timeout: 15000
    });
    await this.page.waitForTimeout(2000);

    const maxWaitTime = 5 * 60 * 1000;
    const checkInterval = 10000;
    const startTime = Date.now();

    while (Date.now() - startTime < maxWaitTime) {
      const currentStatus = await this.getCurrentDagRunStatus();

      if (currentStatus === 'success') {
        return;
      } else if (currentStatus === 'failed') {
        throw new Error(`DAG run failed: ${dagRunId}`);
      }

      await this.page.waitForTimeout(checkInterval);
      await this.page.reload({ waitUntil: 'domcontentloaded' });
      await this.page.waitForTimeout(2000);
    }

    throw new Error(`DAG run did not complete within 5 minutes: ${dagRunId}`);
  }

  private async getCurrentDagRunStatus(): Promise<string> {
    try {
      const statusText = await this.stateElement.textContent().catch(() => '');
      const status = statusText?.trim() || '';

      switch (status) {
        case 'Success':
          return 'success';
        case 'Failed':
          return 'failed';
        case 'Running':
          return 'running';
        case 'Queued':
          return 'queued';
        default:
          return 'unknown';
      }
    } catch (error) {
      return 'unknown';
    }
  }
}
