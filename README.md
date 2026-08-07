# canvas-data-2-aws - WORK IN PROGRESS

This project contains source code and supporting files for a serverless application that you can use to download and maintain a Canvas Data 2 replica database.
You can deploy this application to your AWS account with the SAM CLI. It includes the following files and folders.

- `list_tables` - Code for a Lambda function that fetches the list of CD2 tables using the `dap` client library.
- `sync_table` - Code for a Lambda function that syncs a table using the `dap` client library.
- `init_table` - Code for a Lambda function that inits a table using the `dap` client library.
- `template.yaml` - A template that defines the application's AWS resources.

This application targets version 2.x of the `instructure-dap-client` library. Version 2 changed the
client API in ways that are not backward compatible with 1.x, so the Lambda functions here follow the
same call sequence the `dap` CLI itself uses (`version_upgrade()` followed by
`execute_operation_on_tables()`), and handle the `ExceptionGroup`s that the 2.x client raises.

This application uses an AWS Step Function to orchestrate the workflow:

![workflow diagram](canvas-data-2-step-function.png)

## Application workflow

1. The Step Function is executed on an hourly schedule via EventBridge.
2. The first step executes the `list_tables` Lambda functions which retrieves the list of CD2 tables from the API.
3. The list of tables is passed to a `Map` step which executes the following steps for each item in the list:
   1. The `sync_table` Lambda function is executed. It returns one of `complete`, `needs_init` (the table doesn't exist in the database yet), `needs_ddl_update` (a schema change could not be applied), or `failed`.
   2. The output of `sync_table` is checked. If the table synced, the iteration is complete. If `needs_init` was returned, the `init_table` function is executed. Anything else ends the iteration as a failure.
   3. If executed, the output of `init_table` is checked; `failed` ends the iteration as a failure, anything else as a success.
4. Once all iterations are complete, a notification is sent to an SNS topic summarizing which tables completed and which failed.

## Prerequisites

It will be helpful to have a working knowledge of AWS services and the AWS Console. Before you can deploy the application you will need to have the following available:
* A VPC
* One or more subnets where the Lambda functions can be deployed
* One or more subnets where the database cluster can be deployed (can be the same as the Lambda subnets)
* **Outbound internet access from the Lambda subnets** — see below

### Network access for the Lambda functions

The Lambda functions are attached to your VPC so that they can reach the database, and a
VPC-attached Lambda has no internet access by default. The functions need to reach three things:

| Destination | Why | How to provide it |
| --- | --- | --- |
| `api-gateway.instructure.com` | The DAP API — where the data comes from | **NAT gateway (or equivalent egress).** There is no VPC endpoint for a third-party service. |
| AWS Secrets Manager | Reading the database user credential | NAT gateway, or a [Secrets Manager interface endpoint](https://docs.aws.amazon.com/secretsmanager/latest/userguide/vpc-endpoint-overview.html) |
| AWS SSM Parameter Store | Reading the DAP client ID and secret | NAT gateway, or [VPC endpoints for Systems Manager](https://docs.aws.amazon.com/systems-manager/latest/userguide/setup-create-vpc.html#sysman-setting-up-vpc-create) |

**A NAT gateway is effectively required**, because the DAP API is a public endpoint that no VPC
endpoint can reach. Once you have one, the Secrets Manager and SSM interface endpoints become
optional — they only keep that traffic off the public internet. Without egress, the functions
will simply time out.

The functions also have X-Ray tracing enabled, which needs the same outbound access (or an
X-Ray VPC endpoint).

By default the database will not have a public IP address and will not be accessible outside of your VPC. You will need to configure network access to the database as appropriate for your situation.

## Deploying the application

The Serverless Application Model Command Line Interface (SAM CLI) is an extension of the AWS CLI that adds functionality for building and testing Lambda applications. It uses Docker to run your functions in an Amazon Linux environment that matches Lambda. It can also emulate your application's build environment and API.

To use the SAM CLI to deploy this application, you need the following tools.

* SAM CLI - [Install the SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
* [Python 3 installed](https://www.python.org/downloads/)
* [Docker](https://docs.docker.com/get-docker/) - required, see below

Docker is required because these functions depend on packages with compiled C extensions
(`asyncpg`, `tsv2py`, `aiohttp`). A plain `sam build` installs wheels built for *your* machine,
which produces a deployment package that fails at runtime whenever your platform differs from the
Lambda runtime. `samconfig.toml` therefore sets `use_container = true`, so `sam build` compiles
inside the Lambda-matching container image. If you invoke `sam build` without the config file,
pass `--use-container` explicitly.

The functions are configured for `x86_64`. You can switch them to `arm64` (cheaper and faster on
Graviton) by changing the three `Architectures` entries in `template.yaml`; note that `tsv2py`
publishes no `linux-aarch64` wheels as of 0.8.0, so an arm64 build compiles it from source and
takes longer. Building for an architecture that differs from your host works but runs under
emulation, which is slow.

To build and deploy your application for the first time, run the following in your shell:

```bash
sam build
sam deploy --guided
```

The first command will build the source of your application. The second command will package and deploy your application to AWS, with a series of prompts:

* **Stack Name**: The name of the stack to deploy to CloudFormation. This should be unique to your account and region, and a good starting point would be something matching your project name.
* **AWS Region**: The AWS region you want to deploy your app to.
* **Confirm changes before deploy**: If set to yes, any change sets will be shown to you before execution for manual review. If set to no, the AWS SAM CLI will automatically deploy application changes.
* **Allow SAM CLI IAM role creation**: Many AWS SAM templates, including this example, create AWS IAM roles required for the AWS Lambda function(s) included to access AWS services. By default, these are scoped down to minimum required permissions. To deploy an AWS CloudFormation stack which creates or modifies IAM roles, the `CAPABILITY_IAM` value for `capabilities` must be provided. If permission isn't provided through this prompt, to deploy this example you must explicitly pass `--capabilities CAPABILITY_IAM` to the `sam deploy` command.
* **Save arguments to samconfig.toml**: If set to yes, your choices will be saved to a configuration file inside the project, so that in the future you can just re-run `sam deploy` without parameters to deploy changes to your application.

### Template parameters

Beyond the VPC and subnet parameters, the template takes:

| Parameter | Default | Purpose |
| --- | --- | --- |
| `EngineVersionParameter` | `16.14` | Aurora PostgreSQL engine version |
| `MapMaxConcurrencyParameter` | `10` | How many tables to sync concurrently |
| `DatabaseMinCapacityParameter` | `0.5` | Minimum Aurora Serverless v2 capacity (ACU) |
| `DatabaseMaxCapacityParameter` | `4` | Maximum Aurora Serverless v2 capacity (ACU) |
| `LogRetentionInDaysParameter` | `30` | CloudWatch log retention |
| `SkipTablesParameter` | — | Comma-separated list of tables to skip |

AWS retires pinned Aurora minor versions over time, so if stack creation fails complaining about
the engine version, pick a currently available one:

```bash
aws rds describe-db-engine-versions --engine aurora-postgresql \
  --query 'DBEngineVersions[].EngineVersion' --output text
```

**Concurrency and database capacity are linked.** Each concurrent table sync opens its own
connection, and Aurora Serverless v2 scales `max_connections` with ACU capacity. The defaults are
deliberately conservative because the first run is the heaviest — every table needs to be
initialized at once against a cluster sitting at its minimum capacity. If you see connection
errors, either lower `MapMaxConcurrencyParameter` or raise `DatabaseMinCapacityParameter`. Note
that raising the minimum capacity raises your continuous cost, since it is the floor you pay for
whether or not the workflow is running.

## Preparing the database

Deploying this application will create an AWS Aurora Postgres cluster. A database user credential is also created and stored in AWS Secrets Manager. In order for the application to use that credential to connect to the database,
a Postgresql user must be created and granted appropriate privileges. A helper script is included that will take care of this setup. After deploying the SAM app, run this script:
```
./prepare_aurora_db.py --stack-name <stack name returned by the SAM deployment>
```
Occasionally the schema for a CD2 table will change. The DAP library applies these changes automatically with `ALTER TABLE`, and this application does nothing special to accommodate them.

Note that PostgreSQL refuses to `ALTER TABLE` while a view depends on the table. This application creates no views, so it should not come up — but if you add your own views over the replicated tables, a CD2 schema change will start failing. `sync_table` reports that case as `needs_ddl_update` and the table is listed under `failed_ddl_update` in the SNS notification; you would need to drop the dependent views and re-run the workflow. If you want that handled automatically, the `deps_save_and_drop_dependencies` / `deps_restore_dependencies` functions from https://github.com/rvkulikov/pg-deps-management are one way to do it.

## Monitoring

The workflow publishes a summary to the `WorkflowNotificationTopic` SNS topic at the end of every
run, listing which tables completed and which failed. Subscribe to that topic to receive them.

Because that notification is sent on every run whether or not anything went wrong, a CloudWatch
alarm (`cd2-<environment>-workflow-failed`) also publishes to the same topic when a Step Functions
execution actually fails, so real failures are distinguishable from the routine summaries.

Step Functions execution history is logged to `/aws/vendedlogs/states/cd2-<environment>-refresh`,
and each Lambda function logs to `/aws/lambda/cd2-<environment>-<function>`. All of these use the
retention set by `LogRetentionInDaysParameter`; note that Lambda's default log groups never expire,
which is why the template declares them explicitly. X-Ray tracing is enabled on the functions and
the state machine.

## Configuration

In order for the application to use the DAP API, you will need to provide a client ID and secret.

The application uses AWS SSM Param Store to securely these values and retrieve them at runtime. To store your client ID and secret:
```
aws ssm put-parameter --name '/<environment>/canvas_data_2/dap_client_id' --type SecureString --value '<your client ID>'
aws ssm put-parameter --name '/<environment>/canvas_data_2/dap_client_secret' --type SecureString --value '<your client secret>'
```
where `<environment>` is either `dev` or `prod`. You can also use the AWS SSM console to manage the parameter.

## Running the application

By default the workflow that synchronizes the database will run every three hours. You can also run the workflow manually via the AWS Console: navigate to the Step Functions console, find your `CD2RefreshStateMachine` in the list, and click the `Start execution` button.

This application uses AWS Lambda to run the `init` and `sync` steps for each CD2 table. If the `init` or `sync` step for any given table takes longer than 15 minutes (the limit on how long Lambda functions can run), the workflow will fail. You will be able to see the error in the AWS Step Functions console. If this happens, you'll need to perform the first initialization for the problematic table manually using the DAP client.

TODO: details on how to initialize a table using the DAP client

## Cleanup

To delete the application that you created, use the AWS CLI. Assuming you used your project name for the stack name, you can run the following:

```bash
aws cloudformation delete-stack --stack-name canvas-data-2
```

Alternatively, you can delete the stack in the CloudFormation console (within the AWS web console).
