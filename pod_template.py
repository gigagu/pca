"""
Pod template generation and upload module.

This module handles generation of Spark pod template and upload to MinIO.
"""
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any

import yaml
from minio import Minio
from minio.error import S3Error

# Import from parent package
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
from utils.utils import get_minio_credentials, load_env_config

logger = logging.getLogger(__name__)


def generate_and_upload_pod_template(
    tenant_name: str,
    namespace: str,
    secret_path: str,
    output_dir: Optional[Path] = None,
    env: str = "sit",
    env_config: Optional[Dict[str, Any]] = None
) -> None:
    """
    Generate pod template and upload to MinIO.
    
    This mirrors the Go implementation in pkg/tenant/k8s.go:UpdatePodTemplate
    
    Args:
        tenant_name: Full tenant name
        namespace: Namespace name
        secret_path: Vault secret path for S3 authorizer credentials
        output_dir: Optional output directory for local file generation
        env: Environment name (sit or prod)
        env_config: Environment configuration dictionary (if None, will be loaded)
    """
    # Load environment config if not provided
    if env_config is None:
        env_config = load_env_config(env)
    
    # Generate pod template
    pod_template = build_pod_template(tenant_name, namespace, secret_path, env_config)
    
    # Save locally if output_dir specified
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / f"podtemplate-{tenant_name}.yaml"
        with file_path.open("w", encoding="utf-8") as fh:
            yaml.safe_dump(pod_template, fh, sort_keys=False, default_flow_style=False)
        logger.info(f"Pod template saved to {file_path}")
    
    # Upload to MinIO
    platform_bucket = env_config.get("platform-common-bucket")
    airflow_prefix = env_config.get("airflow-prefix")
    upload_to_minio(tenant_name, pod_template, platform_bucket, airflow_prefix)


def build_pod_template(tenant_name: str, namespace: str, secret_path: str, env_config: Dict[str, Any]) -> dict:
    """
    Build the Kubernetes Pod template dictionary from template file.
    
    Args:
        tenant_name: Fully qualified tenant identifier
        namespace: Tenant namespace
        secret_path: Vault secret path for S3 authorizer credentials
        env_config: Environment configuration dictionary
        
    Returns:
        Pod template dictionary
    """
    # Load template file
    template_dir = Path(__file__).parent.parent / "template"
    template_file = template_dir / "spark-pod-template.yml"
    
    if not template_file.exists():
        raise FileNotFoundError(f"Template file not found: {template_file}")
    
    with template_file.open("r", encoding="utf-8") as f:
        pod_template = yaml.safe_load(f)
    
    # Get environment variables from config
    env_configs = env_config.get("env-configs", {})
    s3_authorizer_url = env_configs.get("S3_AUTHORIZER_SERVER_BASE_URL", "")
    s3_endpoint_url = env_configs.get("S3_ENDPOINT_URL", "")
    hive_endpoint_url = env_configs.get("HIVE_ENDPOINT_URL", "")
    role_name = f"55547_global_app_k8s_{namespace}_role"
    
    # Update metadata
    pod_template["metadata"]["namespace"] = namespace
    pod_template["metadata"]["annotations"]["vault.hashicorp.com/role"] = role_name
    pod_template["metadata"]["annotations"]["vault.hashicorp.com/agent-inject-secret-s3-authorizer-secret"] = secret_path
    pod_template["metadata"]["annotations"]["vault.hashicorp.com/agent-inject-template-s3-authorizer-secret"] = (
        f'{{{{- with secret "{secret_path}" -}}}}\n'
        "{{ .Data.username }}\n"
        "{{ .Data.password }}\n"
        "{{- end }}"
    )
    
    # Update environment variables in containers
    for container in pod_template["spec"]["containers"]:
        # Update env variables
        env_vars = container.get("env", [])
        for env_var in env_vars:
            env_name = env_var.get("name")
            if env_name == "S3_AUTHORIZER_SERVER_BASE_URL":
                env_var["value"] = s3_authorizer_url
            elif env_name == "S3_ENDPOINT_URL":
                env_var["value"] = s3_endpoint_url
            elif env_name == "HIVE_ENDPOINT_URL":
                env_var["value"] = hive_endpoint_url
    
    return pod_template


def upload_to_minio(tenant_name: str, pod_template: dict, platform_bucket: str, airflow_prefix: str) -> None:
    """
    Upload pod template to MinIO.
    
    Args:
        tenant_name: Full tenant name
        pod_template: Pod template dictionary
        platform_bucket: Platform common bucket name
        airflow_prefix: Airflow prefix path
    """
    endpoint, access_key, secret_key = get_minio_credentials()
    
    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=True
    )
    
    object_name = f"{airflow_prefix}/{tenant_name}/templates/spark-pod-template.yml"
    
    # Write to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_file:
        yaml.safe_dump(pod_template, tmp_file, sort_keys=False, default_flow_style=False)
        tmp_path = tmp_file.name
    
    try:
        # Upload to MinIO
        client.fput_object(
            platform_bucket,
            object_name,
            tmp_path,
            content_type="text/yaml"
        )
        logger.info(f"Pod template uploaded to s3://{platform_bucket}/{object_name}")
    except S3Error as e:
        logger.error(f"Failed to upload pod template to MinIO: {e}")
        raise
    finally:
        # Clean up temp file
        import os
        os.unlink(tmp_path)

