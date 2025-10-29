#!/usr/bin/env python3
"""
Test script to verify that deprecation warnings are properly issued
for deprecated Kubernetes configuration options.
"""

import warnings
import tempfile
import os
from unittest.mock import patch

# Add the airflow-core src to the path
import sys
sys.path.insert(0, '/Users/johannes.jungbluth/airflow/airflow-core/src')
sys.path.insert(0, '/Users/johannes.jungbluth/airflow/providers/cncf/kubernetes/src')

def test_deprecation_warnings():
    """Test that deprecation warnings are issued for deprecated configuration options."""
    
    # Create a temporary config file with deprecated options
    config_content = """
[kubernetes_executor]
worker_container_repository = my-repo
worker_container_tag = my-tag
namespace = my-namespace
pod_template_file = 
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.cfg', delete=False) as f:
        f.write(config_content)
        config_file = f.name
    
    try:
        # Mock the configuration to use our test config
        with patch('airflow.configuration.conf') as mock_conf:
            # Set up mock configuration responses
            mock_conf.get.side_effect = lambda section, key, fallback=None: {
                ('kubernetes_executor', 'worker_container_repository'): 'my-repo',
                ('kubernetes_executor', 'worker_container_tag'): 'my-tag', 
                ('kubernetes_executor', 'namespace'): 'my-namespace',
                ('kubernetes_executor', 'pod_template_file'): None,
                ('kubernetes_executor', 'delete_worker_pods'): True,
                ('kubernetes_executor', 'delete_worker_pods_on_failure'): False,
                ('kubernetes_executor', 'worker_pod_pending_fatal_container_state_reasons'): '',
                ('kubernetes_executor', 'worker_pods_creation_batch_size'): 1,
                ('kubernetes_executor', 'multi_namespace_mode'): False,
                ('kubernetes_executor', 'multi_namespace_mode_namespace_list'): None,
                ('kubernetes_executor', 'kube_client_request_args'): {},
                ('kubernetes_executor', 'delete_option_kwargs'): {},
                ('core', 'dags_folder'): '/tmp/dags',
                ('core', 'parallelism'): 32,
            }.get((section, key), fallback)
            
            mock_conf.getboolean.side_effect = lambda section, key, fallback=None: {
                ('kubernetes_executor', 'delete_worker_pods'): True,
                ('kubernetes_executor', 'delete_worker_pods_on_failure'): False,
                ('kubernetes_executor', 'multi_namespace_mode'): False,
            }.get((section, key), fallback)
            
            mock_conf.getint.side_effect = lambda section, key, fallback=None: {
                ('kubernetes_executor', 'worker_pods_creation_batch_size'): 1,
                ('core', 'parallelism'): 32,
            }.get((section, key), fallback)
            
            mock_conf.getjson.side_effect = lambda section, key, fallback=None: {
                ('kubernetes_executor', 'kube_client_request_args'): {},
                ('kubernetes_executor', 'delete_option_kwargs'): {},
            }.get((section, key), fallback)
            
            # Capture warnings
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                
                # Import and instantiate KubeConfig to trigger the warnings
                from airflow.providers.cncf.kubernetes.kube_config import KubeConfig
                config = KubeConfig()
                
                # Check that we got the expected deprecation warnings
                deprecation_warnings = [warning for warning in w if issubclass(warning.category, DeprecationWarning)]
                
                print(f"Found {len(deprecation_warnings)} deprecation warnings:")
                for warning in deprecation_warnings:
                    print(f"  - {warning.message}")
                
                # Verify we have warnings for all three deprecated fields
                expected_warnings = [
                    "worker_container_repository",
                    "worker_container_tag", 
                    "namespace"
                ]
                
                found_warnings = []
                for warning in deprecation_warnings:
                    message = str(warning.message)
                    for expected in expected_warnings:
                        if expected in message:
                            found_warnings.append(expected)
                            break
                
                print(f"\nExpected warnings: {expected_warnings}")
                print(f"Found warnings for: {found_warnings}")
                
                if set(found_warnings) == set(expected_warnings):
                    print("\n✅ SUCCESS: All expected deprecation warnings were issued!")
                    return True
                else:
                    missing = set(expected_warnings) - set(found_warnings)
                    print(f"\n❌ FAILURE: Missing warnings for: {missing}")
                    return False
                    
    finally:
        # Clean up temp file
        os.unlink(config_file)

if __name__ == "__main__":
    success = test_deprecation_warnings()
    sys.exit(0 if success else 1)
