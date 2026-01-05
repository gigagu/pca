#!/usr/bin/env python3
"""
GDP Tenant Onboarding Pipeline - Main Entry Point

This script parses manifest.yml and executes the complete tenant onboarding pipeline.
Each step is implemented as a separate module for maintainability.

Usage:
    python main.py --manifest manifest.yml [--dry-run] [--output-dir ./output] [--env sit|prod] [--ranger-access-manifest ranger-access.yaml]

Requirements:
    pip install pyyaml kubernetes minio
"""
import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

import yaml

# Import step modules
from steps.gdptenant_cr import create_gdptenant_cr
from steps.k8s_resources import create_k8s_resources
from steps.yunikorn_queue import update_yunikorn_queue
from steps.minio_bucket import create_minio_buckets
from steps.trino_resource_group import configure_trino_resource_group
from steps.ranger_policy import apply_ranger_policies
from steps.pod_template import generate_and_upload_pod_template
from steps.trino_ranger_policy import apply_trino_ranger_policies
from utils.utils import generate_tenant_name, generate_namespace, load_env_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_manifest(manifest_path: Path) -> Dict[str, Any]:
    """Parse manifest.yml file."""
    with manifest_path.open("r", encoding="utf-8") as fh:
        manifest = yaml.safe_load(fh)
    
    if not manifest:
        raise ValueError(f"Manifest file {manifest_path} is empty or invalid")
    
    return manifest


def process_tenant(
    platform_config: Dict[str, Any],
    dry_run: bool = False,
    output_dir: Path = None,
    env: str = "sit",
    env_config: Dict[str, Any] = None,
    ranger_access_manifest_path: Path = None
) -> Dict[str, Any]:
    """
    Process a single tenant configuration through the onboarding pipeline.
    
    Returns:
        Dictionary with tenant information and results
    """
    # Extract required fields
    tenant_itam = platform_config.get("tenant-itam")
    tenant_name = platform_config.get("tenant-name")
    
    # Validate required fields
    if not all([tenant_itam, tenant_name]):
        missing = [f for f, v in [("tenant-itam", tenant_itam), ("tenant-name", tenant_name)] if not v]
        raise ValueError(f"Missing required fields: {', '.join(missing)}")
    
    # Generate tenant name and namespace
    tenant_name = generate_tenant_name(tenant_itam, tenant_name)
    namespace = tenant_name

    logger.info(f"Processing tenant: {tenant_name}, namespace: {namespace}")
    
    tenant_info = {
        "tenantName": tenant_name,
        "namespace": namespace,
        "minioBuckets": platform_config.get("object-store-buckets", []),
        "yunikornQueue": tenant_name,
    }
    
    results = {
        "tenant_name": tenant_name,
        "namespace": namespace,
        "steps": {}
    }
    
    try:
        # Deprecated:
        # Step 1: Create GDPTenant CR
        # logger.info(f"[{tenant_name}] Step 1: Creating GDPTenant CR")
        # if not dry_run:
        #     create_gdptenant_cr(tenant_name, platform_config)
        # results["steps"]["gdptenant_cr"] = "success"
        
        # Step 2: Create K8s resources
        logger.info(f"[{tenant_name}] Step 2: Creating K8s resources")
        if not dry_run:
            create_k8s_resources(
                tenant_name,
                namespace,
                platform_config.get("resource-quotas", {}),
                platform_config.get("tenant-noninteractive-owner", []),
                platform_config.get("tenant-noninteractive-viewer", [])
            )
        results["steps"]["k8s_resources"] = "success"
        
        # TODO
        # Step 3: Update YuniKorn queue
        # logger.info(f"[{tenant_name}] Step 3: Updating YuniKorn queue")
        # if not dry_run:
        #     update_yunikorn_queue(
        #         tenant_name,
        #         platform_config.get("jobs-queue", {})
        #     )
        # results["steps"]["yunikorn_queue"] = "success"
        
        # Step 4: Create MinIO buckets
        logger.info(f"[{tenant_name}] Step 4: Creating MinIO buckets")
        if not dry_run:
            create_minio_buckets(platform_config.get("object-store-buckets", []), env)
        results["steps"]["minio_buckets"] = "success"
        
        # Step 5: Configure Trino resource group
        logger.info(f"[{tenant_name}] Step 5: Configuring Trino resource group")
        if not dry_run:
            configure_trino_resource_group(
                tenant_name,
                platform_config.get("tenant-noninteractive-owner", []),
                platform_config.get("tenant-interactive-owner", [])
            )
        results["steps"]["trino_resource_group"] = "success"
        
        # Step 6: Apply Ranger policies
        logger.info(f"[{tenant_name}] Step 6: Applying Ranger policies")
        if not dry_run:
            platform_bucket = env_config.get("platform-common-bucket")
            airflow_prefix = env_config.get("airflow-prefix")
            apply_ranger_policies(
                tenant_name,
                platform_config.get("ranger-policies", {}),
                platform_bucket,
                airflow_prefix,
                env_config
            )
        results["steps"]["ranger_policies"] = "success"
        
        # Step 7: Generate and upload pod template
        logger.info(f"[{tenant_name}] Step 7: Generating pod template")
        
        # Get admin AD account from platform config or use default
        admin_ad_account = platform_config.get("admin-ad-account", "default-admin")
        secret_path = f"scb/ad/zone1/static-cred/{admin_ad_account}"
        if not dry_run:
            generate_and_upload_pod_template(
                tenant_name,
                namespace,
                secret_path,
                output_dir,
                env,
                env_config
            )
        results["steps"]["pod_template"] = "success"
        
        # Step 8: Apply Trino Ranger policies
        logger.info(f"[{tenant_name}] Step 8: Applying Trino Ranger policies")
        if not dry_run:
            if ranger_access_manifest_path and ranger_access_manifest_path.exists():
                apply_trino_ranger_policies(
                    ranger_access_manifest_path,
                    env_config,
                    ranger_service_name="gdp_trino_sit" if env == "sit" else "gdp_trino_prod"
                )
                results["steps"]["trino_ranger_policies"] = "success"
            else:
                if ranger_access_manifest_path:
                    logger.info(f"Ranger access manifest not found at {ranger_access_manifest_path}, skipping Step 8")
                else:
                    logger.info("Ranger access manifest path not provided, skipping Step 8")
                results["steps"]["trino_ranger_policies"] = "skipped"
        
        logger.info(f"[{tenant_name}] All steps completed successfully")
        results["status"] = "success"
        
    except Exception as e:
        logger.error(f"[{tenant_name}] Error in pipeline: {e}", exc_info=True)
        results["status"] = "error"
        results["error"] = str(e)
        raise
    
    return tenant_info


def main():
    parser = argparse.ArgumentParser(
        description="GDP Tenant Onboarding Pipeline"
    )
    parser.add_argument(
        "--manifest",
        required=True,
        type=Path,
        help="Path to manifest.yml file"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no actual changes)"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Output directory for generated files (default: output)"
    )
    parser.add_argument(
        "--env",
        type=str,
        default="sit",
        choices=["sit", "prod"],
        help="Environment name (sit or prod, default: sit)"
    )
    parser.add_argument(
        "--ranger-access-manifest",
        type=Path,
        default=None,
        help="Path to ranger-access.yaml file (optional, Step 8 will be skipped if not provided)"
    )
    args = parser.parse_args()
    
    # Load environment configuration
    try:
        env_config = load_env_config(args.env)
        logger.info(f"Loaded configuration for environment: {args.env}")
    except Exception as e:
        logger.error(f"Failed to load environment configuration: {e}")
        sys.exit(1)
    
    # Validate manifest file
    if not args.manifest.exists():
        logger.error(f"Manifest file not found: {args.manifest}")
        sys.exit(1)
    
    # Parse manifest
    try:
        manifest = parse_manifest(args.manifest)
    except Exception as e:
        logger.error(f"Failed to parse manifest: {e}")
        sys.exit(1)
    
    # Extract platform configurations
    platform_configs = manifest.get("platform-config", [])
    if not platform_configs:
        logger.error("No platform-config found in manifest")
        sys.exit(1)
    
    logger.info(f"Found {len(platform_configs)} tenant configuration(s)")
    
    # Process each tenant
    tenants_info = []
    for idx, pc in enumerate(platform_configs):
        try:
            tenant_info = process_tenant(pc, args.dry_run, args.output_dir, args.env, env_config, args.ranger_access_manifest)
            tenants_info.append(tenant_info)
        except Exception as e:
            logger.error(f"Failed to process platform-config[{idx}]: {e}")
            if args.dry_run:
                continue
            else:
                sys.exit(1)
    
    # Print summary
    print("\n" + "="*60)
    print("ONBOARDING SUMMARY")
    print("="*60)
    for tenant_info in tenants_info:
        print(f"\nTenant: {tenant_info['tenantName']}")
        print(f"  Namespace: {tenant_info['namespace']}")
        print(f"  YuniKorn Queue: {tenant_info['yunikornQueue']}")
        print(f"  MinIO Buckets: {len(tenant_info['minioBuckets'])}")
    print("\n" + "="*60)
    
    if args.dry_run:
        print("\n[DRY RUN] No actual changes were made")
    
    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()

