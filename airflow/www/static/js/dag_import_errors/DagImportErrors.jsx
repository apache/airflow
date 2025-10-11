import React from 'react';
import PropTypes from 'prop-types';

function copyToClipboard(text) {
  navigator.clipboard.writeText(text).catch(() => {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
  });
}

export default function DagImportErrors({ errors = [] }) {
  if (!errors || errors.length === 0) return null;

  return (
    <div className="dag-import-errors">
      {errors.map((err, i) => {
        const path = err.fileloc || err.filename || '';
        return (
          <div className="dag-import-error alert alert-danger" key={`${path}-${i}`}>
            <div className="d-flex align-items-center justify-content-between mb-1">
              <div>
                {err.dag_id ? <strong>{err.dag_id}</strong> : <strong>Broken DAG</strong>}
              </div>
              <div>
                <button
                  type="button"
                  className="btn btn-sm btn-outline-secondary copy-path-btn"
                  title="Copy path"
                  onClick={() => copyToClipboard(path)}
                >
                  Copy
                </button>
              </div>
            </div>
            <pre className="mb-1"><code className="selectable-path">{path}</code></pre>
            {err.stacktrace && (
              <pre className="mb-0 stacktrace small text-muted">{err.stacktrace}</pre>
            )}
          </div>
        );
      })}
    </div>
  );
}

DagImportErrors.propTypes = {
  errors: PropTypes.arrayOf(
    PropTypes.shape({
      fileloc: PropTypes.string,
      filename: PropTypes.string,
      stacktrace: PropTypes.string,
      dag_id: PropTypes.string,
    })
  ),
};
