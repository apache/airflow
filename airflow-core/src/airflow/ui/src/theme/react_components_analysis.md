# Airflow UI React Components Analysis

## Overview
This document analyzes all React components, pages, and layouts in the Airflow UI codebase to identify those that are not following Chakra UI best practices and the custom Chakra UI theme defined in `src/theme.ts`.

## Theme Structure
The custom theme defines semantic color tokens for:
- **Airflow Task States**: success, failed, queued, skipped, up_for_reschedule, up_for_retry, upstream_failed, running, restarting, deferred, scheduled, none, removed
- **Color Palettes**: red, orange, amber, yellow, lime, green, emerald, teal, cyan, sky, blue, indigo, violet, purple, fuchsia, pink, rose, slate, gray, zinc, neutral, stone
- **Special Tokens**: navbar

## Components Directory

### Root Level Components
- `DurationChart.tsx`
- `TrendCountChart.tsx`
- `RenderedJsonField.tsx`
- `ReactMarkdown.tsx`
- `DagVersionSelect.tsx`
- `renderStructuredLog.tsx`
- `Stat.tsx`
- `TaskInstanceTooltip.tsx`
- `TaskName.tsx`
- `TaskTrySelect.tsx`
- `Time.tsx`
- `TimeRangeSelector.tsx`
- `TrendCountButton.tsx`
- `PoolBar.tsx`
- `SearchBar.tsx`
- `LimitedItemsList.tsx`
- `DeleteDialog.tsx`
- `EditableMarkdownArea.tsx`
- `EditableMarkdownButton.tsx`
- `ErrorAlert.tsx`
- `DagVersion.tsx`
- `DagVersionDetails.tsx`
- `ConfigForm.tsx`
- `ConfirmationModal.tsx`
- `DagRunInfo.tsx`
- `TruncatedText.tsx`
- `WarningAlert.tsx`
- `StateIcon.tsx`
- `TogglePause.tsx`
- `QuickFilterButton.tsx`
- `RunTypeIcon.tsx`
- `StateBadge.tsx`
- `HeaderCard.tsx`
- `JsonEditor.tsx`
- `DateTimeInput.tsx`
- `DisplayMarkdownButton.tsx`
- `BreadcrumbStats.tsx`

### Component Subdirectories

#### TriggerDag/
- `TriggerDAGButton.tsx`
- `TriggerDAGForm.tsx`
- `EditableMarkdown.tsx`

#### SearchDags/
- Components in this directory need individual analysis

#### MarkAs/
- `Run/MarkRunAsDialog.tsx`
- `TaskInstance/MarkTaskInstanceAsDialog.tsx`

#### DataTable/
- `ToggleTableDisplay.tsx`

#### Clear/
- `Run/ClearRunDialog.tsx`
- `TaskInstance/ClearTaskInstanceDialog.tsx`

#### Graph/
- `TaskNode.tsx`

#### FlexibleForm/
- `FieldBool.tsx`

#### DagActions/
- `FavoriteDagButton.tsx`
- `DeleteDagButton.tsx`
- `RunBackfillForm.tsx`

#### Assets/
- `AssetEvents.tsx`
- `AssetNode.tsx`

#### Banner/
- `BackfillBanner.tsx`

#### ActionAccordion/
- `ActionAccordion.tsx`

#### AssetExpression/
- `AssetNode.tsx`

#### ui/
- `SegmentedControl.tsx`
- `ActionButton.tsx`

## Pages Directory

### Root Level Pages
- `Iframe.tsx`
- `DagRuns.tsx`
- `Security.tsx`
- `Providers.tsx`
- `ReactPlugin.tsx`
- `Plugins.tsx`
- `DeleteRunButton.tsx`
- `Error.tsx`
- `ExternalView.tsx`

### Page Subdirectories

#### Dashboard/
- `Stats/Stats.tsx`
- `HistoricalMetrics/TaskInstanceMetrics.tsx`
- `HistoricalMetrics/DagRunMetrics.tsx`

#### Variables/
- `Variables.tsx`
- `ImportVariablesForm.tsx`
- `ImportVariablesButton.tsx`
- `DeleteVariablesButton.tsx`
- `ManageVariable/VariableForm.tsx`
- `ManageVariable/AddVariableButton.tsx`
- `ManageVariable/DeleteVariableButton.tsx`

#### Connections/
- `Connections.tsx`
- `AddConnectionButton.tsx`
- `DeleteConnectionButton.tsx`
- `DeleteConnectionsButton.tsx`
- `ConnectionForm.tsx`
- `TestConnectionButton.tsx`

#### TaskInstances/
- `TaskInstancesFilter.tsx`
- `DeleteTaskInstanceButton.tsx`

#### Task/
- `Overview/Overview.tsx`

#### TaskInstance/
- `ExtraLinks.tsx`
- `Logs/ExternalLogLink.tsx`

#### Asset/
- `CreateAssetEventModal.tsx`
- `CreateAssetEvent.tsx`

#### Pools/
- `AddPoolButton.tsx`
- `DeletePoolButton.tsx`
- `PoolForm.tsx`

#### DagsList/
- `DagsFilters/PausedFilter.tsx`
- `DagsFilters/FavoriteFilter.tsx`
- `DagsFilters/TagFilter.tsx`

#### Dag/
- `Overview/Overview.tsx`

## Layouts Directory

### Root Level Layouts
- `BaseLayout.tsx`
- `DagsLayout.tsx`

### Layout Subdirectories

#### Details/
- `DetailsLayout.tsx`
- `PanelButtons.tsx`

#### Nav/
- Components in this directory need individual analysis

## Assets Directory
- `AirflowPin.tsx`