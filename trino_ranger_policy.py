"""
Trino Ranger policy configuration module.

This module handles creation of Apache Ranger policies for Trino access.
"""
import logging
import os
from pathlib import Path
from typing import List, Dict, Any

import requests
import urllib3
import yaml

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


def normalize_to_list(value: Any) -> List[str]:
    """
    Normalize a value to a list of strings.
    
    If value is already a list, return it as is.
    If value is a string, return it as a single-item list.
    If value is None or empty, return an empty list.
    
    Args:
        value: Value to normalize (can be list, string, or None)
        
    Returns:
        List of strings
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, list):
        return [str(item) for item in value if item]
    return []


def parse_ranger_access_manifest(manifest_path: Path) -> Dict[str, Any]:
    """
    Parse ranger-access.yaml manifest file.
    
    Args:
        manifest_path: Path to ranger-access.yaml file
        
    Returns:
        Dictionary containing parsed manifest data
    """
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest file not found: {manifest_path}")
    
    with manifest_path.open("r", encoding="utf-8") as f:
        manifest = yaml.safe_load(f)
    
    if not manifest:
        raise ValueError(f"Manifest file {manifest_path} is empty or invalid")
    
    return manifest


def apply_trino_ranger_policies(
    manifest_path: Path,
    env_config: Dict[str, Any],
    ranger_service_name: str = "gdp_trino_sit"
) -> None:
    """
    Apply Trino Ranger policies from ranger-access.yaml manifest.
    
    Creates two policies:
    1. Trino-policy-{tenant-itam}-{tenant-name}-fullaccess: for all "all" permission schemas
    2. Trino-policy-{tenant-itam}-{tenant-name}-readonlyaccess: for all "readOnly" permission schemas
    
    Args:
        manifest_path: Path to ranger-access.yaml file
        env_config: Environment configuration dictionary
        ranger_service_name: Ranger service name (default: gdp_trino_sit)
    """
    # Parse manifest
    manifest = parse_ranger_access_manifest(manifest_path)
    
    gdp_config = manifest.get("gdp-ranger-config", {})
    schemas = manifest.get("schemas", [])
    
    if not schemas:
        logger.info("No schemas found in manifest, skipping Trino Ranger policies")
        return
    
    # Get tenant information
    tenant_itam = gdp_config.get("tenant-itam", "")
    tenant_name = gdp_config.get("tenant-name", "")
    
    if not tenant_itam or not tenant_name:
        raise ValueError("tenant-itam and tenant-name are required in gdp-ranger-config")
    
    # Get Trino catalog from config
    trino_catalog = env_config.get("trino_catalog", "gdp_global")
    
    # Get Ranger base URL
    ranger_base_url = env_config.get("env-configs", {}).get("RANGER_BASE_URL")
    if not ranger_base_url:
        raise ValueError("RANGER_BASE_URL not found in environment configuration")
    
    # Group schemas by permission type
    fullaccess_schemas = []
    fullaccess_groups = []
    readonly_schemas = []
    readonly_groups = []
    
    for schema_config in schemas:
        schema_name = schema_config.get("schema-name")
        permission_type = schema_config.get("permission-type", "readOnly")
        ad_groups = normalize_to_list(schema_config.get("ad-groups", []))
        
        if not schema_name:
            logger.warning("Skipping schema with no schema-name")
            continue
        
        if not ad_groups:
            logger.warning(f"Skipping schema {schema_name} with no ad-groups")
            continue
        
        if permission_type.lower() == "all":
            fullaccess_schemas.append(schema_name)
            fullaccess_groups.extend(ad_groups)
        else:  # readOnly
            readonly_schemas.append(schema_name)
            readonly_groups.extend(ad_groups)
    
    # Remove duplicate groups while preserving order
    fullaccess_groups = list(dict.fromkeys(fullaccess_groups))
    readonly_groups = list(dict.fromkeys(readonly_groups))
    
    # Create fullaccess policy
    if fullaccess_schemas and fullaccess_groups:
        fullaccess_policy_name = f"Trino-policy-{tenant_itam}-{tenant_name}-fullaccess"
        create_trino_policy(
            policy_name=fullaccess_policy_name,
            service_name=ranger_service_name,
            catalog=trino_catalog,
            schemas=fullaccess_schemas,
            groups=fullaccess_groups,
            access_types=["select", "insert", "create", "drop", "delete", "all"],
            ranger_base_url=ranger_base_url
        )
        logger.info(f"Trino Ranger fullaccess policy {fullaccess_policy_name} created for schemas: {fullaccess_schemas}")
    
    # Create readonly policy
    if readonly_schemas and readonly_groups:
        readonly_policy_name = f"Trino-policy-{tenant_itam}-{tenant_name}-readonlyaccess"
        create_trino_policy(
            policy_name=readonly_policy_name,
            service_name=ranger_service_name,
            catalog=trino_catalog,
            schemas=readonly_schemas,
            groups=readonly_groups,
            access_types=["select", "show"],
            ranger_base_url=ranger_base_url
        )
        logger.info(f"Trino Ranger readonly policy {readonly_policy_name} created for schemas: {readonly_schemas}")
    
    logger.info(f"All Trino Ranger policies created successfully")


def post_policy(policy: Dict[str, Any], func_name: str, ranger_base_url: str) -> None:
    """Post a policy to Ranger API."""
    url = f"{ranger_base_url}/service/plugins/policies/apply"
    
    username = os.getenv("RANGER_USER")
    password = os.getenv("RANGER_PASS")
    
    if not username or not password:
        raise ValueError("RANGER_USER and RANGER_PASS environment variables required")
    
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(
            url,
            json=policy,
            headers=headers,
            auth=(username, password),
            verify=False,
            timeout=30
        )
        response.raise_for_status()
        logger.debug(f"{func_name} policy applied successfully")
    except Exception as e:
        logger.error(f"{func_name} failed: {e}")
        raise


def create_trino_policy(
    policy_name: str,
    service_name: str,
    catalog: str,
    schemas: List[str],
    groups: List[str],
    access_types: List[str],
    ranger_base_url: str
) -> None:
    """
    Create a Trino Ranger policy.
    
    Args:
        policy_name: Policy name
        service_name: Ranger service name (e.g., gdp_trino_sit)
        catalog: Trino catalog name
        schemas: List of Trino schema names
        groups: List of AD groups
        access_types: List of access types (e.g., ["select", "insert"])
        ranger_base_url: Ranger base URL
    """
    if not groups:
        logger.warning(f"No groups specified for policy {policy_name}, skipping")
        return
    
    if not schemas:
        logger.warning(f"No schemas specified for policy {policy_name}, skipping")
        return
    
    # Build access list
    accesses = [{"type": access_type, "isAllowed": True} for access_type in access_types]
    
    policy = {
        "isEnabled": True,
        "service": service_name,
        "name": policy_name,
        "policyType": 0,
        "policyPriority": 0,
        "isAuditEnabled": True,
        "resources": {
            "catalog": {
                "values": [catalog],
                "isRecursive": False
            },
            "schema": {
                "values": schemas,
                "isRecursive": False
            },
            "table": {
                "values": ["*"],
                "isRecursive": False
            },
            "column": {
                "values": ["*"],
                "isRecursive": False
            }
        },
        "policyItems": [{
            "accesses": accesses,
            "groups": groups,
            "delegateAdmin": False
        }],
        "serviceType": "trino",
        "isDenyAllElse": False
    }
    
    post_policy(policy, f"CreateTrinoPolicy-{policy_name}", ranger_base_url)

