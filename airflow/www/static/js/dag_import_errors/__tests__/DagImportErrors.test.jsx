import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import DagImportErrors from '../DagImportErrors';


Object.assign(navigator, {
clipboard: {
writeText: jest.fn().mockResolvedValue(undefined),
},
});


describe('DagImportErrors component', () => {
it('renders path and copy button and copies to clipboard', async () => {
const errors = [
{
fileloc: '/opt/airflow/dags/pipeline_main_daily_dwh_dag/airflow/pipeline_main_daily_dwh_dag.py',
stacktrace: 'Traceback (most recent call last): ...',
dag_id: 'pipeline_main_daily_dwh',
},
];


render(<DagImportErrors errors={errors} />);


const pathEl = screen.getByText(/pipeline_main_daily_dwh_dag.py/);
expect(pathEl).toBeInTheDocument();

const copyBtn = screen.getByText('Copy');
expect(copyBtn).toBeInTheDocument();


fireEvent.click(copyBtn);
expect(navigator.clipboard.writeText).toHaveBeenCalledWith(errors[0].fileloc);
});
});