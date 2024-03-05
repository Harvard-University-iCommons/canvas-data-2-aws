import asyncio
import os

import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities import parameters
from aws_lambda_powertools.utilities.typing import LambdaContext
from botocore.config import Config
from dap.api import DAPClient
from dap.dap_types import Credentials
from dap.integration.database import DatabaseConnection
from dap.integration.database_errors import NonExistingTableError
from dap.replicator.sql import SQLReplicator
from pysqlsync.base import QueryException

region = os.environ.get("AWS_REGION")

config = Config(region_name=region)
ssm_provider = parameters.SSMProvider(config=config)

logger = Logger()

env = os.environ.get("ENV", "dev")
db_user_secret_name = os.environ.get("DB_USER_SECRET_NAME")

db_cluster_arn = os.environ.get("DB_CLUSTER_ARN")
admin_secret_arn = os.environ.get("ADMIN_SECRET_ARN")

param_path = f"/{env}/canvas_data_2"

api_base_url = os.environ.get("API_BASE_URL", "https://api-gateway.instructure.com")

namespace = "canvas"

rds_data_client = boto3.client("rds-data")


def lambda_handler(event, context: LambdaContext):
    params = ssm_provider.get_multiple(param_path, max_age=600, decrypt=True)

    dap_client_id = params["dap_client_id"]
    dap_client_secret = params["dap_client_secret"]

    db_user_secret = parameters.get_secret(db_user_secret_name, transform="json")
    db_user = db_user_secret["username"]
    db_password = quote_plus(db_user_secret["password"])
    db_name = db_user_secret["dbname"]
    db_host = db_user_secret["host"]
    db_port = db_user_secret["port"]

    conn_str = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    db_connection = DatabaseConnection(connection_string=conn_str)

    credentials = Credentials.create(
        client_id=dap_client_id, client_secret=dap_client_secret
    )

    table_name = event["table_name"]

    logger.info(f"syncing table: {table_name}")

    os.chdir("/tmp/")

    try:
        asyncio.get_event_loop().run_until_complete(
            sync_table(credentials, api_base_url, db_connection, namespace, table_name)
        )

        event["state"] = "complete"
    except QueryException as e:
        logger.exception(f"{e}")
        if "ALTER TABLE" in str(e):
            # This is a special case where the table needs a DDL update
            # Before we can apply the DDL update, we need to drop all dependent views
            try:
                drop_dependencies(db_name="cd2", table_name=table_name)
                asyncio.get_event_loop().run_until_complete(
                    sync_table(
                        credentials, api_base_url, db_connection, namespace, table_name
                    )
                )
                event["state"] = "complete_with_update"
            except Exception as e:
                logger.exception(e)
                event["state"] = "failed"
            finally:
                restore_dependencies(db_name="cd2", table_name=table_name)
        else:
            event["state"] = "failed"
    except NonExistingTableError as e:
        logger.exception(e)
        event["state"] = "needs_init"
    except ValueError as e:
        logger.exception(e)
        if "table not initialized" in str(e):
            event["state"] = "needs_init"
        else:
            event["state"] = "failed"
    except Exception as e:
        logger.exception(e)
        event["state"] = "failed"

    logger.info(f"event: {event}")

    return event


async def sync_table(credentials, api_base_url, db_connection, namespace, table_name):
    async with DAPClient(api_base_url, credentials) as session:
        await SQLReplicator(session, db_connection).synchronize(namespace, table_name)


def drop_dependencies(db_name, table_name):
    # This function will drop all dependent views and retain the DDL to recreate them
    pass
    drop_sql = f"""select public.deps_save_and_drop_dependencies(
        'canvas',
        '{table_name}',
        '{{
          "dry_run": false,
          "verbose": false,
          "populate_materialized_view": false
        }}'
      )
    """
    response = rds_data_client.execute_statement(
        secretArn=admin_secret_arn,
        database=db_name,
        resourceArn=db_cluster_arn,
        sql=drop_sql,
    )
    logger.info(f"dropped dependencies for {table_name}: {response}")


def restore_dependencies(db_name, table_name):
    # This function will restore all dependent views
    pass
    restore_sql = f"""
      select public.deps_restore_dependencies(
        'canvas',
        '{table_name}',
        '{{
          "dry_run": false,
          "verbose": false
        }}'
      )
    """
    response = rds_data_client.execute_statement(
        secretArn=admin_secret_arn,
        database=db_name,
        resourceArn=db_cluster_arn,
        sql=restore_sql,
    )
    logger.info(f"restored dependencies for {table_name}: {response}")
