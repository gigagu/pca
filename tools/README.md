# Tools

This directory contains utility tools for the GDP Tenant Onboarding Pipeline.

## Batch Onboarding Processor

Batch tenant onboarding processing tool for handling multiple tenant onboarding requests.

### Directory Structure

The tool expects the following directory structure:
```
t1_onboarding_request/
  tenant1/
    tenant-onboarding.yaml
    ranger-access.yaml (optional)
  tenant2/
    tenant-onboarding.yaml
    ranger-access.yaml (optional)
```

### Usage

```bash
# Run from project root
python src/tools/batch_onboarding.py --base-dir t1_onboarding_request --env sit

# Run from tools directory
cd src/tools
python batch_onboarding.py --base-dir ../../t1_onboarding_request --env sit
```

### Command Line Arguments

| Argument | Required | Description | Default |
|----------|----------|-------------|---------|
| `--base-dir` | ✅ Yes | Base directory containing tenant subdirectories | - |
| `--env` | ❌ No | Environment name (sit or prod) | `sit` |
| `--dry-run` | ❌ No | Run in dry-run mode | `False` |
| `--output-dir` | ❌ No | Output directory | `{base-dir}/output/{tenant-name}` |
| `--continue-on-error` | ❌ No | Continue processing other tenants if one fails | `False` |

### Examples

```bash
# Basic usage
python batch_onboarding.py --base-dir t1_onboarding_request --env sit

# Dry-run mode
python batch_onboarding.py --base-dir t1_onboarding_request --env sit --dry-run

# Specify output directory
python batch_onboarding.py --base-dir t1_onboarding_request --env sit --output-dir ./custom_output

# Continue on error
python batch_onboarding.py --base-dir t1_onboarding_request --env sit --continue-on-error
```

### Output

The script will for each tenant:
1. Call `main.py` to process `tenant-onboarding.yaml`
2. If `ranger-access.yaml` exists, automatically pass it to `main.py`
3. Save output to `{output-dir}/{tenant-name}/` directory

### Processing Results

The script will display:
- Processing status (success/failure) for each tenant
- Final processing summary
- Error messages for failed tenants

---

## Pod Template Generator

Standalone tool for generating Spark Pod template YAML files from `manifest.yml` files. This tool can be used independently of the full onboarding process, specifically for generating Kubernetes Pod template configurations.

### Features

- 📝 Read platform configuration from `manifest.yml` files
- 🔄 Automatically generate Pod templates for each `platform-config` entry
- 🎯 Automatically generate tenant names and namespaces
- 🔐 Integrated Vault secret injection configuration
- 📦 Support batch generation of multiple Pod templates
- 💾 Output standard Kubernetes YAML format

### Requirements

#### Python Version
- Python 3.6+

#### Dependencies
```bash
pip install pyyaml
```

Or install all dependencies from project root:
```bash
pip install -r ../requirements.txt
```

### Usage

#### Basic Usage

```bash
cd python/src/tools
python pod_template_generator.py --manifest ../../onboarding_manifest/manifest.yml
```

#### Specify Output Directory

```bash
python pod_template_generator.py \
    --manifest ../../onboarding_manifest/manifest.yml \
    --output-dir ./generated_templates
```

#### Print to Standard Output

```bash
python pod_template_generator.py \
    --manifest ../../onboarding_manifest/manifest.yml \
    --output-dir ./output \
    --print
```

#### Run from Project Root

```bash
python python/src/tools/pod_template_generator.py \
    --manifest python/onboarding_manifest/manifest.yml \
    --output-dir ./output
```

### Command Line Arguments

| Argument | Required | Description | Default |
|----------|----------|-------------|---------|
| `--manifest` | ✅ Yes | Path to manifest.yml file | - |
| `--output-dir` | ❌ No | Output directory path | `./output` |
| `--print` | ❌ No | Also print YAML to standard output | `False` |

### Manifest File Format

The tool expects `manifest.yml` files to contain a `platform-config` array, with each entry requiring the following fields:

#### Required Fields

- `itam`: ITAM number (integer)
- `tenant`: Tenant name (string)
- `tenant-short-name`: Tenant short name (string)

#### Optional Fields

- `namespace`: Namespace suffix (string, defaults to `"default"`)
- `hcv-secret-path`: Vault secret path (string, for S3 authorizer credentials)

#### Example Manifest

```yaml
platform-config:
  - itam: 55547
    tenant: gdpapp
    tenant-short-name: app
    namespace: analytics
    hcv-secret-path: "gdp/data/s3-authorizer/tenant/app"
  - itam: 55547
    tenant: another-tenant
    tenant-short-name: tenant2
    namespace: default
    hcv-secret-path: "gdp/data/s3-authorizer/tenant/tenant2"
```

### Output Format

#### File Naming Rules

Generated Pod template files follow this naming format:
```
podtemplate-{tenant_name}.yaml
```

Where `tenant_name` is generated as:
```
t-{itam}-{tenant}-{tenant-short-name}
```

Example:
- `itam: 55547`, `tenant: gdpapp`, `tenant-short-name: app` 
- → Generated file: `podtemplate-t-55547-gdpapp-app.yaml`

#### Namespace Generation Rules

The namespace in Pod templates is generated as:
```
{tenant_name}-{namespace_suffix}
```

Example:
- `tenant_name: t-55547-gdpapp-app`, `namespace: analytics`
- → Namespace: `t-55547-gdpapp-app-analytics`

### Generated Pod Template Features

Generated Pod templates include the following configurations:

#### Scheduler Configuration
- **Scheduler**: `yunikorn`
- **Queue**: Automatically set to tenant name
- **Task Group**: `sched-style`

#### Security Configuration
- **Run as User**: `10000`
- **Run as Group**: `10000`
- **Supplemental Groups**: `[10000]`
- **Run as Non-Root**: `true`
- **Disallow Privilege Escalation**: `true`
- **Drop All Capabilities**: `true`
- **Seccomp Profile**: `RuntimeDefault`

#### Vault Integration
- **Vault Agent Injection**: Enabled
- **Service Account**: `vault-auth`
- **Role Name**: `55547_global_app_k8s_{namespace}_role`
- **S3 Authorizer Secret**: Injected from `hcv-secret-path`

#### Environment Variables
- `POD_NAME`: Pod name (from metadata.name)
- `S3_AUTHORIZER_AUTH_FILE`: S3 authorizer authentication file path
- `HIVE_ENDPOINT_URL`: Hive Metastore endpoint
- `S3_AUTHORIZER_SERVER_BASE_URL`: S3 authorizer server address
- `S3_ENDPOINT_URL`: MinIO endpoint address
- `AWS_ACCESS_KEY`: `NO_NEED` (uses Vault-injected credentials)
- `AWS_SECRET_KEY`: `NO_NEED` (uses Vault-injected credentials)
- `DATAHUB_REST_TOKEN`: From Secret `datahub-credential`
- `DATAHUB_REST_ENDPOINT`: From Secret `datahub-credential`

#### Volume Mounts
- `gdp-truststore-volume`: GDP truststore (`/etc/gdp/ssl`)
- `k8s-sa-token`: Kubernetes service account token
- `s3auth-client-certs-vol`: S3 authorizer client certificates

#### Resource Limits
- **Requests**:
  - CPU: `1`
  - Memory: `1Gi`
- **Limits**:
  - CPU: `1`
  - Memory: `2Gi`

### Usage Examples

#### Example 1: Basic Generation

```bash
cd python/src/tools
python pod_template_generator.py \
    --manifest ../../onboarding_manifest/CEMSCOMP_ranger-access.yaml
```

Output:
```
2024-01-01 10:00:00 - INFO - Successfully generated 1 pod template(s):
2024-01-01 10:00:00 - INFO -   - output/podtemplate-t-55547-cemscomp-comp.yaml
```

#### Example 2: Custom Output Directory with Print

```bash
python pod_template_generator.py \
    --manifest ../../onboarding_manifest/manifest.yml \
    --output-dir ./my_templates \
    --print
```

#### Example 3: Processing Multiple Platform Configurations

If the manifest contains multiple `platform-config` entries, the tool will generate one Pod template for each entry:

```yaml
platform-config:
  - itam: 55547
    tenant: tenant1
    tenant-short-name: t1
  - itam: 55547
    tenant: tenant2
    tenant-short-name: t2
```

Will generate:
- `podtemplate-t-55547-tenant1-t1.yaml`
- `podtemplate-t-55547-tenant2-t2.yaml`

### Error Handling

#### Common Errors

1. **Manifest File Not Found**
   ```
   ERROR - Manifest file not found: /path/to/manifest.yml
   ```
   - Check if the file path is correct
   - Ensure using absolute path or correct relative path

2. **Missing Required Fields**
   ```
   WARNING - Skipping platform-config[0] - missing fields: itam, tenant
   ```
   - Check `platform-config` entries in the manifest file
   - Ensure `itam`, `tenant`, and `tenant-short-name` fields are included

3. **No platform-config**
   ```
   ERROR - No platform-config found in manifest
   ```
   - Ensure the manifest file contains a `platform-config` key
   - Check if the YAML format is correct

4. **YAML Parse Error**
   ```
   ERROR - Failed to parse manifest: ...
   ```
   - Check if YAML syntax is correct
   - Use YAML validation tools to check file format

### Relationship to Full Pipeline

This tool is a standalone implementation of **Step 7** (Pod template generation) in the full onboarding process. Key differences:

| Feature | Standalone Tool | Full Pipeline |
|---------|----------------|---------------|
| Input | manifest.yml | manifest.yml |
| Output | Local YAML files | Local files + MinIO upload |
| Dependencies | Only pyyaml | Full dependencies (K8s, MinIO, etc.) |
| Purpose | Template generation only | Full automation pipeline |

### Notes

1. **Standalone Operation**: This tool does not depend on Kubernetes clusters or MinIO services and can run offline
2. **File Overwrite**: If a file with the same name already exists in the output directory, it will be overwritten
3. **Field Validation**: `platform-config` entries missing required fields will be skipped and will not cause the entire process to fail
4. **Namespace Default**: If `namespace` is not specified, `"default"` will be used as the suffix

### Related Files

- **Full Pipeline**: `../main.py` - Complete tenant onboarding pipeline
- **Pod Template Module**: `../steps/pod_template.py` - Pod template generation step in the pipeline
- **Example Manifests**: `../../onboarding_manifest/` - Example manifest files

### License

Same as the main project.
