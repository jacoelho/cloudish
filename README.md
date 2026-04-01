# Cloudish

Cloudish is an AWS-compatible local emulator.

## Build

Build the full workspace with all services enabled:

```sh
cargo build --workspace --all-features
```

## Run

Cloudish uses Cloudish-specific variables for server startup:

```sh
export CLOUDISH_DEFAULT_ACCOUNT=000000000000
export CLOUDISH_DEFAULT_REGION=eu-west-2
export CLOUDISH_STATE_DIR=.cloudish/state
export CLOUDISH_EDGE_WIRE_LOG=on

cargo run -p app
```

`CLOUDISH_EDGE_WIRE_LOG` controls JSON edge-request logging. Accepted values
are `on`/`off`, `true`/`false`, `yes`/`no`, and `1`/`0`. Leave it unset or set
it to `off` to disable wire logging.

Check that the emulator is up:

```sh
curl http://localhost:4566/__cloudish/health
```

Cloudish binds to `127.0.0.1:4566` by default. To run on a different address
or port, set the app-owned bind variables before starting the server:

```sh
export CLOUDISH_DEFAULT_ACCOUNT=000000000000
export CLOUDISH_DEFAULT_REGION=eu-west-2
export CLOUDISH_STATE_DIR=.cloudish/state
export CLOUDISH_EDGE_HOST=0.0.0.0
export CLOUDISH_EDGE_PORT=4570
export CLOUDISH_EDGE_WIRE_LOG=on

cargo run -p app
```

Then use the matching address in your health check and client endpoint. For
example, when binding to all interfaces on port `4570`, connect through the
local loopback address:

```sh
curl http://127.0.0.1:4570/__cloudish/health
```

`CLOUDISH_EDGE_HOST` expects an IP address such as `127.0.0.1`, `0.0.0.0`, or
`::1`.

## Use With AWS CLI

Use AWS-standard client variables for credentials, region, and endpoint:

```sh
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_DEFAULT_REGION="us-east-1"
export AWS_ENDPOINT_URL="http://localhost:4566"
```

If you start Cloudish on a different bind address or port, update
`AWS_ENDPOINT_URL` to match. For the custom bind example above:

```sh
export AWS_ENDPOINT_URL="http://127.0.0.1:4570"
```

Cloudish also supports an unsafe bootstrap-only auth bypass for local debugging:

```sh
export CLOUDISH_UNSAFE_BOOTSTRAP_AUTH="on"
```

Accepted values are `on`/`off`, `true`/`false`, `yes`/`no`, and `1`/`0`.
Leave it unset or set it to `off` to keep normal bootstrap SigV4 validation.

When enabled, requests signed with the built-in bootstrap access key id `test`
continue even if their SigV4 signature does not match. This does not disable
IAM access key or STS session credential validation, and it applies only to
the built-in bootstrap access key id `test`.

Example AWS CLI commands:

```sh
aws sts get-caller-identity

aws s3api create-bucket --bucket cloudish-demo
aws s3api list-buckets

echo "hello from cloudish" > demo.txt
aws s3 cp demo.txt s3://cloudish-demo/demo.txt
aws s3api get-object --bucket cloudish-demo --key demo.txt demo-copy.txt
cat demo-copy.txt
```

If you prefer to make the endpoint explicit per command, use:

```sh
aws --endpoint-url "$AWS_ENDPOINT_URL" sts get-caller-identity
```

## Tests

Run the acceptance suite:

```sh
cargo test -p tests
```

Run one acceptance story:

```sh
cargo test -p tests --test s3_core
```

## Docker

Build a container image for the `app` binary:

```sh
docker build -f build/Dockerfile -t cloudish .
```

Build a multi-arch image with Buildx:

```sh
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f build/Dockerfile \
  -t cloudish \
  --push \
  .
```

Run the container image with the baked server defaults:

```sh
docker run --rm -p 4566:4566 -v cloudish-state:/var/lib/cloudish cloudish
```

The image enables edge wire logging by default with
`CLOUDISH_EDGE_WIRE_LOG=on`. Override it at runtime if you want a quieter
container:

```sh
docker run --rm \
  -p 4566:4566 \
  -e CLOUDISH_EDGE_WIRE_LOG=off \
  -v cloudish-state:/var/lib/cloudish \
  cloudish
```

Omit the `-v` flag if you want disposable state instead of a persisted data
directory.
