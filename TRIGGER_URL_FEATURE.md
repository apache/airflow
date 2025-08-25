# Trigger Form URL Feature

This feature allows you to directly access the trigger form for a DAG via URL with pre-populated fields, similar to the functionality that existed in Airflow 2.7+.

## Overview

The trigger form can now be accessed directly via URL, and URL parameters can be used to pre-populate form fields. This enables:

1. Direct linking to trigger forms
2. Pre-population of configuration parameters
3. Integration with external systems that need to trigger DAGs with specific parameters

## URL Structure

The trigger form can be accessed at:
```
/dags/{dagId}/trigger
```

Where `{dagId}` is the ID of the DAG you want to trigger.

## URL Parameters

The following URL parameters can be used to pre-populate form fields:

- `conf`: JSON configuration string for the DAG run
- `dag_run_id`: Custom run ID for the DAG run
- `logical_date`: Logical date for the DAG run (ISO format)
- `note`: Note/description for the DAG run

## Examples

### Basic Trigger Form
```
http://localhost:8080/dags/example_dag/trigger
```

### Trigger Form with Pre-populated Configuration
```
http://localhost:8080/dags/example_dag/trigger?conf={"param1":"value1","param2":"value2"}
```

### Trigger Form with Multiple Parameters
```
http://localhost:8080/dags/example_dag/trigger?conf={"param1":"value1"}&dag_run_id=manual_run_001&logical_date=2024-01-15T10:00:00.000&note=Triggered%20from%20external%20system
```

## Implementation Details

### New Components

1. **Trigger Page** (`src/pages/Dag/Trigger.tsx`): A full-page trigger form that supports URL parameters
2. **Router Configuration**: Added route `/dags/:dagId/trigger` to the router
3. **Navigation**: Added "Trigger" tab to the DAG details page

### Key Features

- **URL Parameter Parsing**: Uses `useSearchParams` to read URL parameters
- **Form Pre-population**: Automatically populates form fields based on URL parameters
- **Validation**: Maintains all existing validation logic
- **Navigation**: Provides back button to return to DAG details
- **Responsive Design**: Works on both desktop and mobile devices

### URL Parameter Handling

The component reads URL parameters using React Router's `useSearchParams` hook:

```typescript
const urlConf = searchParams.get("conf");
const urlDagRunId = searchParams.get("dag_run_id");
const urlLogicalDate = searchParams.get("logical_date");
const urlNote = searchParams.get("note");
```

These parameters are then used to set the default values for the form fields.

## Usage Scenarios

1. **External System Integration**: External systems can generate URLs with specific parameters to trigger DAGs
2. **Bookmarking**: Users can bookmark trigger forms with specific configurations
3. **Documentation**: Documentation can include direct links to trigger forms with example parameters
4. **Testing**: QA teams can easily test DAG triggers with specific parameters

## Security Considerations

- URL parameters are visible in the browser address bar
- Sensitive information should not be passed via URL parameters
- The form still requires proper authentication and authorization
- All existing security measures remain in place

## Migration from Airflow 2.7+

This feature restores functionality that was available in Airflow 2.7+ but was missing in Airflow 3. The implementation is compatible with the previous URL structure and parameter format.

## Testing

To test the feature:

1. Navigate to a DAG details page
2. Click on the "Trigger" tab
3. Try accessing the trigger form directly via URL
4. Test with various URL parameters
5. Verify that form fields are pre-populated correctly

## Future Enhancements

Potential future enhancements could include:

- Support for additional URL parameters
- URL parameter validation
- Custom parameter schemas
- Integration with external parameter stores
