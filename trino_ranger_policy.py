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
    
    # Get Trino catalog from config
    trino_catalog = env_config.get("trino_catalog", "gdp_global")
    
    # Get Ranger base URL
    ranger_base_url = env_config.get("env-configs", {}).get("RANGER_BASE_URL")
    if not ranger_base_url:
        raise ValueError("RANGER_BASE_URL not found in environment configuration")
    
    # Process each schema
    for schema_config in schemas:
        schema_name = schema_config.get("schema-name")
        table_names = normalize_to_list(schema_config.get("table-names", ["*"]))
        column_names = normalize_to_list(schema_config.get("column-names", ["*"]))
        permission_type = schema_config.get("permission-type", "readOnly")
        ad_groups = normalize_to_list(schema_config.get("ad-groups", []))
        
        if not schema_name:
            logger.warning("Skipping schema with no schema-name")
            continue
        
        if not ad_groups:
            logger.warning(f"Skipping schema {schema_name} with no ad-groups")
            continue
        
        # Determine access types based on permission type
        if permission_type.lower() == "all":
            access_types = ["select", "insert", "create", "drop", "delete", "all"]
        else:  # readOnly
            access_types = ["select", "show"]
        
        # Create policy name
        policy_name = f"trino-{schema_name}-{permission_type.lower()}"
        
        # Create Trino policy
        create_trino_policy(
            policy_name=policy_name,
            service_name=ranger_service_name,
            catalog=trino_catalog,
            schema=schema_name,
            tables=table_names,
            columns=column_names,
            groups=ad_groups,
            access_types=access_types,
            ranger_base_url=ranger_base_url
        )
        
        logger.info(f"Trino Ranger policy {policy_name} created for schema {schema_name}")
    
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
    schema: str,
    tables: List[str],
    columns: List[str],
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
        schema: Trino schema name
        tables: List of table names (can include "*" for all)
        columns: List of column names (can include "*" for all)
        groups: List of AD groups
        access_types: List of access types (e.g., ["select", "insert"])
        ranger_base_url: Ranger base URL
    """
    if not groups:
        logger.warning(f"No groups specified for policy {policy_name}, skipping")
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
                "values": [schema],
                "isRecursive": False
            },
            "table": {
                "values": tables,
                "isRecursive": False
            },
            "column": {
                "values": columns,
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

