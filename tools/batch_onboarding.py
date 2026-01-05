#!/usr/bin/env python3
"""
Batch tenant onboarding script.

This script processes multiple tenant onboarding requests from a directory structure:
t1_onboarding_request/
  tenant1/
    tenant-onboarding.yaml
    ranger-access.yaml (optional)
  tenant2/
    tenant-onboarding.yaml
    ranger-access.yaml (optional)
"""
import argparse
import logging
import subprocess
import sys
from pathlib import Path
from typing import List, Dict, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def find_tenant_configs(base_dir: Path) -> List[Dict[str, Path]]:
    """
    Find all tenant configurations in the base directory.
    
    Args:
        base_dir: Base directory containing tenant subdirectories
        
    Returns:
        List of dictionaries with tenant_name, manifest_path, and ranger_access_path
    """
    tenant_configs = []
    
    if not base_dir.exists():
        logger.error(f"Base directory not found: {base_dir}")
        return tenant_configs
    
    # Iterate through subdirectories
    for tenant_dir in sorted(base_dir.iterdir()):
        if not tenant_dir.is_dir():
            continue
        
        tenant_name = tenant_dir.name
        manifest_path = tenant_dir / "tenant-onboarding.yaml"
        ranger_access_path = tenant_dir / "ranger-access.yaml"
        
        # Check if manifest exists
        if not manifest_path.exists():
            logger.warning(f"Skipping {tenant_name}: tenant-onboarding.yaml not found")
            continue
        
        config = {
            "tenant_name": tenant_name,
            "manifest_path": manifest_path,
            "ranger_access_path": ranger_access_path if ranger_access_path.exists() else None
        }
        tenant_configs.append(config)
        logger.info(f"Found tenant config: {tenant_name}")
    
    return tenant_configs


def run_onboarding(
    manifest_path: Path,
    ranger_access_path: Path = None,
    env: str = "sit",
    dry_run: bool = False,
    output_dir: Path = None
) -> Tuple[bool, str]:
    """
    Run onboarding for a single tenant.
    
    Args:
        manifest_path: Path to tenant-onboarding.yaml
        ranger_access_path: Path to ranger-access.yaml (optional)
        env: Environment name (sit or prod)
        dry_run: Whether to run in dry-run mode
        output_dir: Output directory for generated files
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    # Get the main.py script path
    script_dir = Path(__file__).parent.parent
    main_script = script_dir / "main.py"
    
    if not main_script.exists():
        return False, f"main.py not found at {main_script}"
    
    # Build command
    cmd = [sys.executable, str(main_script), "--manifest", str(manifest_path), "--env", env]
    
    if dry_run:
        cmd.append("--dry-run")
    
    if output_dir:
        cmd.extend(["--output-dir", str(output_dir)])
    
    if ranger_access_path and ranger_access_path.exists():
        cmd.extend(["--ranger-access-manifest", str(ranger_access_path)])
    
    try:
        logger.info(f"Running command: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            cwd=str(script_dir),
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode == 0:
            return True, "Success"
        else:
            error_msg = result.stderr or result.stdout or "Unknown error"
            return False, error_msg
    
    except Exception as e:
        return False, str(e)


def main():
    parser = argparse.ArgumentParser(
        description="Batch tenant onboarding processor"
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        required=True,
        help="Base directory containing tenant subdirectories (e.g., t1_onboarding_request)"
    )
    parser.add_argument(
        "--env",
        type=str,
        default="sit",
        choices=["sit", "prod"],
        help="Environment name (sit or prod, default: sit)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no actual changes)"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for generated files (default: {base-dir}/output/{tenant-name})"
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue processing other tenants if one fails"
    )
    args = parser.parse_args()
    
    # Find all tenant configurations
    logger.info(f"Scanning directory: {args.base_dir}")
    tenant_configs = find_tenant_configs(args.base_dir)
    
    if not tenant_configs:
        logger.error("No tenant configurations found")
        sys.exit(1)
    
    logger.info(f"Found {len(tenant_configs)} tenant configuration(s)")
    
    # Process each tenant
    results = []
    for config in tenant_configs:
        tenant_name = config["tenant_name"]
        manifest_path = config["manifest_path"]
        ranger_access_path = config["ranger_access_path"]
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing tenant: {tenant_name}")
        logger.info(f"{'='*60}")
        
        # Determine output directory
        if args.output_dir:
            tenant_output_dir = args.output_dir / tenant_name
        else:
            tenant_output_dir = args.base_dir / "output" / tenant_name
        
        # Run onboarding
        success, message = run_onboarding(
            manifest_path=manifest_path,
            ranger_access_path=ranger_access_path,
            env=args.env,
            dry_run=args.dry_run,
            output_dir=tenant_output_dir
        )
        
        if success:
            logger.info(f"✓ Tenant {tenant_name} processed successfully")
            results.append({"tenant": tenant_name, "status": "success"})
        else:
            logger.error(f"✗ Tenant {tenant_name} failed: {message}")
            results.append({"tenant": tenant_name, "status": "error", "error": message})
            
            if not args.continue_on_error:
                logger.error("Stopping batch processing due to error")
                break
    
    # Print summary
    print("\n" + "="*60)
    print("BATCH PROCESSING SUMMARY")
    print("="*60)
    success_count = sum(1 for r in results if r["status"] == "success")
    error_count = len(results) - success_count
    
    print(f"\nTotal tenants: {len(results)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {error_count}")
    
    print("\nDetails:")
    for result in results:
        status_icon = "✓" if result["status"] == "success" else "✗"
        print(f"  {status_icon} {result['tenant']}: {result['status']}")
        if result["status"] == "error" and "error" in result:
            print(f"    Error: {result['error'][:100]}...")
    
    print("="*60)
    
    # Exit with error code if any failed
    if error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

