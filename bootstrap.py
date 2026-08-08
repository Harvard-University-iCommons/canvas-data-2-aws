#!/usr/bin/env python

"""Prepare a deployed canvas-data-2 stack for use.

Two jobs, both of which have to happen after `sam deploy` and before the workflow
can run:

  1. Store the DAP API client ID and secret in SSM Parameter Store.
  2. Create the database user and schemas that the Lambda functions connect as.

Both are idempotent, so it is safe to re-run.
"""

import json
from typing import Any

import boto3
import click
from botocore.exceptions import ClientError
from rich.console import Console

console = Console()


def read_stack(stack_name: str) -> tuple[dict[str, str], dict[str, str]]:
    """Return the outputs and parameters of the named CloudFormation stack."""
    stack = boto3.resource("cloudformation").Stack(stack_name)
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.outputs}
    parameters = {p["ParameterKey"]: p["ParameterValue"] for p in stack.parameters}
    return outputs, parameters


def existing_parameters(ssm: Any, names: list[str]) -> set[str]:
    """Return the subset of `names` that already exist in Parameter Store."""
    response = ssm.get_parameters(Names=names, WithDecryption=False)
    return {p["Name"] for p in response["Parameters"]}


def configure_dap_credentials(
    ssm: Any,
    env: str,
    client_id: str | None,
    client_secret: str | None,
    update: bool,
) -> None:
    """Store the DAP client ID and secret as SecureString parameters."""
    path = f"/{env}/canvas_data_2"
    id_name = f"{path}/dap_client_id"
    secret_name = f"{path}/dap_client_secret"

    already_set = existing_parameters(ssm, [id_name, secret_name])
    if already_set and not update and not (client_id or client_secret):
        console.print(
            f" - DAP credentials already present under [bold]{path}[/bold]; "
            "pass --update-credentials to replace them",
            style="green",
        )
        return

    # Prompt only for what was not supplied on the command line, so that the secret
    # need never appear in shell history or in the process list.
    if client_id is None:
        client_id = click.prompt("DAP client ID", type=str)
    if client_secret is None:
        client_secret = click.prompt(
            "DAP client secret", type=str, hide_input=True, confirmation_prompt=True
        )

    # No KeyId is specified, so these are encrypted with the AWS-managed
    # alias/aws/ssm key. That is deliberate: ListTablesFunction is granted
    # SSMParameterReadPolicy but no KMS permissions, so encrypting these with the
    # stack's own customer-managed key would break it at runtime.
    for name, value in ((id_name, client_id), (secret_name, client_secret)):
        try:
            ssm.put_parameter(
                Name=name, Value=value, Type="SecureString", Overwrite=True
            )
            console.print(f" - Stored [bold]{name}[/bold]", style="bold green")
        except ClientError as e:
            console.print(f" ! Could not store {name}: {e}", style="bold red")


def configure_database_users(
    secrets_client: Any,
    rds_data_client: Any,
    env: str,
    admin_secret_arn: str,
    admin_username: str,
    aurora_cluster_arn: str,
) -> None:
    """Create a database user, its schema, and the instructure_dap schema."""
    secret_name_prefix = f"cd2-db-user-{env}-"
    user_secrets = secrets_client.list_secrets(
        Filters=[{"Key": "name", "Values": [secret_name_prefix]}],
        MaxResults=100,
    )

    if not user_secrets["SecretList"]:
        console.print(
            f" ! No secrets found matching {secret_name_prefix}. Is the stack deployed "
            "and is EnvironmentParameter what you expect?",
            style="bold red",
        )
        return

    for s in user_secrets["SecretList"]:
        secret_arn = s["ARN"]

        secret_value = json.loads(
            secrets_client.get_secret_value(SecretId=secret_arn)["SecretString"]
        )
        password = secret_value["password"]
        username = secret_value["username"]
        database_name = secret_value["dbname"]

        console.print(
            f" - creating database user [bold]{username}[/bold] in database [bold]{database_name}[/bold]",
            style="green",
        )

        # create the database user
        try:
            user_sql = f"CREATE USER {username} WITH PASSWORD '{password}' LOGIN"
            rds_data_client.execute_statement(
                resourceArn=aurora_cluster_arn,
                secretArn=admin_secret_arn,
                sql=user_sql,
                database=database_name,
            )
            console.print(" - Created user", style="bold green")
        except ClientError as e:
            if "already exists" in e.response["Error"]["Message"]:
                console.print(f" - User {username} already exists", style="bold red")

                try:
                    change_sql = f"ALTER USER {username} WITH PASSWORD '{password}'"
                    rds_data_client.execute_statement(
                        resourceArn=aurora_cluster_arn,
                        secretArn=admin_secret_arn,
                        sql=change_sql,
                        database=database_name,
                    )
                    console.print(
                        f" - Updated password for user {username}", style="bold green"
                    )
                except ClientError as e:
                    console.print(
                        f" ! Unexpected error when updating password for {username}: {e}",
                        style="bold red",
                    )
                    continue
            else:
                console.print(
                    f" ! Unexpected error when creating user {username}: {e}",
                    style="bold red",
                )
                continue

        # Grant the role to the admin user
        try:
            grant_sql = f"GRANT {username} TO {admin_username}"
            rds_data_client.execute_statement(
                resourceArn=aurora_cluster_arn,
                secretArn=admin_secret_arn,
                sql=grant_sql,
                database=database_name,
            )
            console.print(
                f" - Granted user {username} to {admin_username}", style="bold green"
            )
        except ClientError as e:
            console.print(
                f" ! Unexpected error granting {username} role to {admin_username}: {e}",
                style="bold red",
            )
            continue

        # create the schema
        try:
            schema_sql = f"CREATE SCHEMA IF NOT EXISTS AUTHORIZATION {username}"
            rds_data_client.execute_statement(
                resourceArn=aurora_cluster_arn,
                secretArn=admin_secret_arn,
                sql=schema_sql,
                database=database_name,
            )
            console.print(f" - Created schema [bold]{username}[/bold]", style="green")
        except ClientError as e:
            console.print(
                f" ! Unexpected error creating schema {username}: {e}", style="bold red"
            )
            continue

        # create the instructure_dap schema
        try:
            schema_sql = (
                f"CREATE SCHEMA IF NOT EXISTS instructure_dap AUTHORIZATION {username}"
            )
            rds_data_client.execute_statement(
                resourceArn=aurora_cluster_arn,
                secretArn=admin_secret_arn,
                sql=schema_sql,
                database=database_name,
            )
            console.print(
                f" - Created schema [bold]instructure_dap[/bold] in database [bold]{database_name}[/bold]",
                style="green",
            )
        except ClientError as e:
            console.print(f" ! Unexpected error: {e}", style="bold red")
            continue

        # grant create permission on database to canvas user
        try:
            grant_sql = f"GRANT CREATE ON DATABASE {database_name} TO {username}"
            rds_data_client.execute_statement(
                resourceArn=aurora_cluster_arn,
                secretArn=admin_secret_arn,
                sql=grant_sql,
                database="postgres",
            )
            console.print(
                f" - Granted CREATE on database {database_name} to user {username}",
                style="bold green",
            )
        except ClientError as e:
            console.print(f" ! Unexpected error: {e}", style="bold red")


@click.command()
@click.option(
    "--stack-name",
    required=True,
    help="the name of the CloudFormation stack containing the cd2 Aurora database",
)
@click.option(
    "--dap-client-id",
    default=None,
    help="DAP API client ID. Prompted for if not supplied.",
)
@click.option(
    "--dap-client-secret",
    default=None,
    help="DAP API client secret. Prompted for (hidden) if not supplied.",
)
@click.option(
    "--update-credentials",
    is_flag=True,
    help="replace the DAP credentials even if they are already stored",
)
@click.option(
    "--skip-credentials",
    is_flag=True,
    help="only prepare the database; leave the DAP credentials alone",
)
@click.option(
    "--skip-database",
    is_flag=True,
    help="only store the DAP credentials; leave the database alone",
)
def main(
    stack_name: str,
    dap_client_id: str | None,
    dap_client_secret: str | None,
    update_credentials: bool,
    skip_credentials: bool,
    skip_database: bool,
) -> None:
    """Prepare a deployed canvas-data-2 stack for use."""
    outputs, parameters = read_stack(stack_name)
    env = parameters["EnvironmentParameter"]

    if not skip_credentials:
        console.print("Storing DAP API credentials", style="bold green")
        configure_dap_credentials(
            boto3.client("ssm"),
            env,
            dap_client_id,
            dap_client_secret,
            update_credentials,
        )

    if not skip_database:
        console.print("Starting database preparation", style="bold green")
        secrets_client = boto3.client("secretsmanager")
        admin_secret_arn = outputs["AdminSecretArn"]
        admin_secret = json.loads(
            secrets_client.get_secret_value(SecretId=admin_secret_arn)["SecretString"]
        )
        configure_database_users(
            secrets_client,
            boto3.client("rds-data"),
            env,
            admin_secret_arn,
            admin_secret["username"],
            outputs["AuroraClusterArn"],
        )


if __name__ == "__main__":
    main()
