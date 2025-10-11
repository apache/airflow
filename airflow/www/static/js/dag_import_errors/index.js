import DagImportErrors from './DagImportErrors';
export default DagImportErrors;


import React from 'react';
import { createRoot } from 'react-dom/client';


window.mountDagImportErrors = function mountDagImportErrors(elementId = 'dag-import-errors-root') {
const el = document.getElementById(elementId);
if (!el) return null;
let errors = [];
try {
errors = JSON.parse(el.dataset.errors || '[]');
} catch (e) {
errors = [];
}
const root = createRoot(el);
root.render(React.createElement(DagImportErrors, { errors }));
return root;
};