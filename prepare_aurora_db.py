#!/usr/bin/env python

import argparse
import json

import boto3
from rich.console import Console
from botocore.exceptions import ClientError

parser = argparse.ArgumentParser()
parser.add_argument(
    "--stack-name",
    help="the name of the CloudFormation stack containing the cd2 Aurora database",
    required=True,
)

args = parser.parse_args()

console = Console()

secrets_client = boto3.client("secretsmanager")
rds_data_client = boto3.client("rds-data")
cf_reource = boto3.resource("cloudformation")
stack = cf_reource.Stack(args.stack_name)

console.print("Starting database preparation", style="bold green")

# read the outputs and parameters from the Cloudformation stack
stack_outputs = {
    output["OutputKey"]: output["OutputValue"]
    for output in stack.outputs
}
stack_parameters = {
    parameter["ParameterKey"]: parameter["ParameterValue"]
    for parameter in stack.parameters
}

# get the database admin secret
admin_secret_arn = stack_outputs["AdminSecretArn"]
admin_secret = json.loads(
    secrets_client.get_secret_value(SecretId=admin_secret_arn)["SecretString"]
)
admin_username = admin_secret["username"]

# get the database cluster ARN
aurora_cluster_arn = stack_outputs["AuroraClusterArn"]

# get the environment
env = stack_parameters["EnvironmentParameter"]


# get the database user secrets
secret_name_prefix = f"uw-cd2-db-user-{env}-"
user_secrets = secrets_client.list_secrets(
    Filters=[{"Key": "name", "Values": [secret_name_prefix]}],
    MaxResults=100,
)

# for each user secret, create the database user and schema
for s in user_secrets["SecretList"]:
    secret_arn = s["ARN"]

    secret_value = json.loads(
        secrets_client.get_secret_value(SecretId=secret_arn)["SecretString"]
    )
    password = secret_value["password"]
    username = secret_value["username"]
    database_name = secret_value["dbname"]
    schema_name = username

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
                    f" ! Unexpected error when updating password for {username}: {e}", style="bold red"
                )
                continue
        else:
            console.print(
                f" ! Unexpected error when creating user {username}: {e}", style="bold red"
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
        console.print(f" ! Unexpected error granting {username} role to {admin_username}: {e}", style="bold red")
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
        console.print(f" ! Unexpected error creating schema {username}: {e}", style="bold red")
        continue
