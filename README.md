# ceph3_data_manager

Simple Flask app to browse YAML config files in S3 and view one flattened row per experiment.

## Features

- Read from a configurable S3 bucket
- Discover experiment Hydra files (`config.yaml`, `hydra.yaml`, `overrides.yaml`) in nested S3 paths
- Merge them with OmegaConf/Hydra-style semantics, including override dotlists
- Resolve interpolation values before display
- Show one row per experiment with flattened keys as columns
- Display loading progress and support aborting a running load

## Requirements

- Python 3.11+
- `uv`
- AWS credentials available to `boto3` (env vars, profile, or IAM role)

## Setup

```bash
uv venv .venv
source .venv/bin/activate
uv pip install -e .
```

## Run

```bash
source .venv/bin/activate
uv run flask --app app run --debug --port 8000
```

Open <http://127.0.0.1:8000> and provide:

- Bucket name
- Optional prefix
- Optional AWS region
- Optional S3 endpoint URL (useful for Ceph/MinIO or other S3-compatible APIs)

Credentials are loaded from your local `s3cmd` configuration file at `~/.s3cfg`.

## Galvani Ceph S3 Notes

The app is configured to be compatible with the Galvani Ceph S3 guidance:

- Uses path-style S3 addressing
- Uses checksum behavior compatible with Ceph (`when_required`)
- Supports both Galvani endpoints:
  - Public: `https://s3.mlcloud.uni-tuebingen.de`
  - Private: `http://192.168.213.99`
- Defaults region to `us-east-1` if not set
- Reads `access_key`, `secret_key`, `host_base`, and `use_https` from `~/.s3cfg`

In the UI, use the endpoint preset buttons to fill the two Galvani endpoints quickly.

You can also preconfigure environment variables:

```bash
export S3_BUCKET=my-bucket
export S3_PREFIX=team-a/
export AWS_REGION=us-east-1
export S3_ENDPOINT_URL=https://s3.example.local
uv run flask --app app run --debug --port 8000
```

For Galvani, configure credentials once with `s3cmd --configure` so `~/.s3cfg` contains your access values.
