# canvas-data-2-aws

A serverless application that builds and maintains a PostgreSQL replica of your Canvas Data 2 data.
A Step Function runs every three hours, syncing each CD2 table into an Aurora database using
Instructure's `dap` client library. Deploy it to your AWS account with the SAM CLI.

**This is a reference implementation.** It deploys and runs as-is, but it makes deliberately simple
choices you will want to revisit before using it with real data — see
[Adapting this for your own environment](#adapting-this-for-your-own-environment).

## Quick start

This path creates everything, including its own VPC, so you need no existing AWS networking.

**Before you start, you need:**

* **[SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)** — builds and deploys the stack
* **[uv](https://docs.astral.sh/uv/getting-started/installation/)** — manages the local Python environment. You do not need to install Python; uv fetches its own.
* **[Docker](https://docs.docker.com/get-docker/)** — `sam build` compiles the functions' dependencies inside a container
* **A DAP API client ID and secret**, from [identity.instructure.com](https://identity.instructure.com)
* **AWS credentials** with permission to create VPC, RDS, Lambda, Step Functions, IAM, KMS, Secrets
  Manager and SSM resources. The SAM CLI uses the same credentials as the AWS CLI, so run
  `aws configure` (or `aws sso login --profile <name>` if your organization uses IAM Identity
  Center) and confirm with `aws sts get-caller-identity`. See
  [Authentication and access credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-authentication.html)
  for the other options.

**1. Create the local environment**

```bash
uv sync
```

**2. Build and deploy**

```bash
sam build
sam deploy --guided
```

At the prompts, set `CreateNetworkParameter` to `Yes` and accept the defaults for
`VpcIdParameter`, `DatabaseSubnetListParameter` and `LambdaSubnetListParameter` — leaving them
blank is what tells the stack to build its own network. Answer `y` to allow IAM role creation, and
save your answers to `samconfig.toml` so later deploys need no prompting.

This creates a VPC with two public and two private subnets across two Availability Zones, one NAT
gateway, and the Aurora cluster. Their IDs are stack outputs.

**3. Prepare the stack**

```bash
uv run bootstrap.py --stack-name <the stack name you chose>
```

This prompts for your DAP client ID and secret, stores them, and creates the database user and
schemas. It is safe to re-run.

**4. Run it**

The workflow runs every three hours on its own. To trigger it immediately, open the Step Functions
console, select `CD2RefreshStateMachine`, and choose **Start execution**.

The first run initializes every table and takes considerably longer than later incremental syncs.
Results are published to the stack's SNS topic — subscribe to it to get them.

> **Cost.** The NAT gateway this creates is billed hourly whether or not the workflow runs — about
> $33/month in `us-east-1`, plus $0.045/GB — and the Aurora cluster has a continuous floor of its
> own. If you are only evaluating, [delete the stack](#cleanup) when you are done.

## How it works

![workflow diagram](canvas-data-2-step-function.png)

EventBridge starts the Step Function on a schedule. `list_tables` fetches the CD2 table list, and a
`Map` state processes each table concurrently: `sync_table` performs an incremental sync, and if the
table does not exist yet it returns `needs_init` and the workflow runs `init_table` instead. When
every table is done, a summary is published to SNS.

| Path | What it is |
| --- | --- |
| `list_tables/` | Lambda: fetches the list of CD2 tables |
| `sync_table/` | Lambda: incrementally syncs one table |
| `init_table/` | Lambda: performs the first full load of one table |
| `template.yaml` | All AWS resources |
| `bootstrap.py` | Post-deploy setup: DAP credentials, database user and schemas |

## Deploying into an existing VPC

Leave `CreateNetworkParameter` at its `No` default and supply your own network instead. You will
need:

* A VPC
* One or more subnets for the Lambda functions
* **At least two subnets in different Availability Zones** for the database (they can be the same
  subnets). RDS requires this even though the template creates a single database instance;
  otherwise stack creation fails with `DB Subnet Group doesn't meet availability zone coverage
  requirement.`
* **Outbound internet access from the Lambda subnets**, which in practice means a NAT gateway

The functions run inside your VPC and so have no internet access by default. They need to reach:

| Destination | For | Provided by |
| --- | --- | --- |
| `api-gateway.instructure.com` | The DAP API | **NAT gateway only** — no VPC endpoint can reach a third-party service |
| AWS Secrets Manager | The database credential | NAT gateway, or an interface endpoint |
| AWS SSM Parameter Store | The DAP credentials | NAT gateway, or an interface endpoint |

Because the DAP API is a public endpoint, a NAT gateway is effectively required; the two interface
endpoints are then optional and only keep that traffic off the public internet. Without egress the
functions simply time out. X-Ray tracing needs the same access.

## Configuration reference

### Template parameters

| Parameter | Default | Purpose |
| --- | --- | --- |
| `CreateNetworkParameter` | `No` | Create a VPC, subnets and NAT gateway for this stack |
| `VpcCidrParameter` | `10.0.0.0/16` | CIDR for the created VPC (ignored unless creating one) |
| `VpcIdParameter` | — | Required when not creating the network |
| `DatabaseSubnetListParameter` | — | Required when not creating the network |
| `LambdaSubnetListParameter` | — | Required when not creating the network |
| `EngineVersionParameter` | `16.14` | Aurora PostgreSQL version — the `dap` client requires 16.3 or later |
| `DatabaseMinCapacityParameter` | `0.5` | Minimum Aurora Serverless v2 capacity (ACU) |
| `DatabaseMaxCapacityParameter` | `4` | Maximum Aurora Serverless v2 capacity (ACU) |
| `LogRetentionInDaysParameter` | `30` | CloudWatch log retention |
| `SkipTablesParameter` | — | Comma-separated list of tables to skip |
| `EnvironmentParameter` | `dev` | `dev` or `prod`; namespaces resource names and the SSM path |

If stack creation fails on the engine version, AWS has retired that minor release — pick a current
one with `aws rds describe-db-engine-versions --engine aurora-postgresql`.

### bootstrap.py

| Flag | Effect |
| --- | --- |
| `--stack-name` | Required. The deployed stack to prepare. |
| `--update-credentials` | Replace DAP credentials that are already stored |
| `--dap-client-id`, `--dap-client-secret` | Supply credentials non-interactively |
| `--skip-credentials` | Only prepare the database |
| `--skip-database` | Only store the credentials |

It needs permission to read the stack, read and write the `/<environment>/canvas_data_2` SSM
parameters, read the stack's secrets, and call the RDS Data API. It reaches the database through
the RDS Data API rather than a direct connection, so it can run from outside the VPC.

To set the DAP credentials by hand instead, they are two SecureString parameters under
`/<environment>/canvas_data_2/`, named `dap_client_id` and `dap_client_secret`. Leave them
encrypted with the default `alias/aws/ssm` key — `ListTablesFunction` has parameter read access but
no KMS permissions, so a customer-managed key breaks it at runtime.

## Monitoring

Every run publishes a summary to the stack's SNS topic listing which tables completed and which
failed. Because that arrives whether or not anything went wrong, a CloudWatch alarm
(`cd2-<environment>-workflow-failed`) publishes to the same topic when an execution actually fails.

Step Functions execution history goes to `/aws/vendedlogs/states/cd2-<environment>-refresh` and each
function logs to `/aws/lambda/cd2-<environment>-<function>`, both retained for
`LogRetentionInDaysParameter` days. X-Ray tracing is enabled throughout.

## Operating notes

**Large tables can exceed Lambda's 15-minute limit.** If a table's init or sync does, the workflow
fails and the error appears in the Step Functions console. That table has to be initialized
out-of-band with the `dap` CLI.

TODO: details on how to initialize a table using the DAP client

**Schema changes are handled automatically.** The DAP library applies them with `ALTER TABLE`, and
this application does nothing special to accommodate them. PostgreSQL does refuse to `ALTER TABLE`
while a view depends on the table — this application creates no views, so it should not arise, but
if you add your own, `sync_table` reports the table as `needs_ddl_update` and lists it under
`failed_ddl_update` in the notification. You would drop the dependent views and re-run.

**Concurrency and database capacity are linked.** Each concurrent sync opens its own connection, and
Aurora Serverless v2 scales `max_connections` with capacity. If you see connection errors, raise
`DatabaseMinCapacityParameter` or lower the Map state's `MaxConcurrency` (currently `10`) in
`template.yaml`. Raising the minimum capacity raises your bill continuously.

## Cleanup

```bash
aws cloudformation delete-stack --stack-name <your stack name>
```

This removes the NAT gateway and releases its Elastic IP, which is what stops the hourly charge.

Two things are left behind on purpose, and you should deal with both:

**The DAP credentials.** `bootstrap.py` creates these outside CloudFormation, so deleting the stack
does not remove them — your client secret stays in Parameter Store until you delete it:

```bash
aws ssm delete-parameters --names \
  "/<environment>/canvas_data_2/dap_client_id" \
  "/<environment>/canvas_data_2/dap_client_secret"
```

where `<environment>` matches the stack's `EnvironmentParameter`.

**The database snapshot.** `AWS::RDS::DBCluster` defaults to `DeletionPolicy: Snapshot`, so the
cluster is retained as a final snapshot. Delete it from the RDS console or with
`aws rds delete-db-cluster-snapshot` if you do not want to keep paying for its storage.

## Adapting this for your own environment

The choices below suit a demonstration rather than any particular institution's production
environment, roughly in the order they tend to matter.

### Assumptions in the design

* **One Canvas instance, one database, one schema.** The functions read a single pair of DAP
  credentials from a fixed SSM path and replicate the `canvas` namespace into a single database.
  Supporting multiple instances or tenants means threading an identifier through the Step Function
  payload and the SSM paths.
* **No views over the replicated tables.** See *Operating notes*.
* **Two environments, `dev` and `prod`,** defined in `samconfig.toml` and enforced by
  `EnvironmentParameter`.

### Security

Hardening is minimal here so the template stays readable. Before using real data, consider:

* **KMS key**: no automatic rotation, no alias, and the default key policy.
* **Only the secrets are encrypted.** Log groups and the SNS topic are not; they carry table names,
  row counts and error messages rather than Canvas data. Extending encryption to them needs key
  policy grants for the CloudWatch Logs and CloudWatch Alarms service principals — Logs encrypts
  through its own service principal rather than the writing role, and an alarm that cannot use the
  key stops notifying with an error visible only in its alarm history.
* **Security groups** declare no egress rules, so the default allow-all applies. The database group
  also has a `TODO` for whatever ingress your analysts or BI tools need.
* **`DeletionProtection` is `false`** on the database cluster.
* **Secrets are never rotated.** The database credential is generated once at deploy time.
* **The RDS Data API is enabled** because `bootstrap.py` uses it. It is IAM-gated, but it is an
  additional path to the database; if you create the database user another way, turn it off.

### Cost

* **The NAT gateway is usually the largest fixed cost**, frequently more than the database at low
  usage. The template creates one, not one per AZ, so losing an AZ stops the workflow.
* **Aurora Serverless v2 minimum capacity is a continuous charge**, not a ceiling.
* **Interface endpoints** for Secrets Manager and SSM each carry their own hourly charge.

### Sizing and limits

* **Lambda's 15-minute ceiling** is the most likely reason to outgrow this architecture and move the
  work to ECS or Batch.
* **Function memory** was chosen by estimate, not measurement. `init_table` runs at 8192 MB largely
  to get proportional CPU. Measure before assuming these fit your data.
* **The schedule is every three hours.** CD2 data is not real-time, so syncing more often mostly
  costs money; less often makes each incremental sync heavier.

### Build and deployment

* **Docker is required** because the functions depend on compiled extensions (`asyncpg`, `tsv2py`,
  `aiohttp`). `samconfig.toml` sets `use_container = true` so `sam build` compiles inside the
  Lambda-matching image; building without it produces wheels for your machine and a package that
  fails at runtime. Pass `--use-container` explicitly if you build without the config file.
* **`x86_64` versus `arm64`.** The functions are `x86_64`. Switching the three `Architectures`
  entries to `arm64` is cheaper to run, but `tsv2py` publishes no `linux-aarch64` wheels as of
  0.8.0, so it compiles from source and builds take longer. Building for an architecture other than
  your host works but runs under emulation.
* **Resource names are prefixed `cd2-`.** Deploying two instances of this stack into one account
  will collide; add your own prefix.
* **The stack exports the cluster and admin secret ARNs.** Renaming those exports later will block
  stack updates if anything imports them.
* **There is no CI.** Consider running `sam validate --lint`, `ruff check` and `sam build` on pull
  requests.
