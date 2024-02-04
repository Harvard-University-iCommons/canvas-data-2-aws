import asyncio
import os
from urllib.parse import quote_plus

import dap.plugins
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities import parameters
from aws_lambda_powertools.utilities.typing import LambdaContext
from botocore.config import Config
from dap.actions.sync_db import sync_db
from dap.dap_types import Credentials
from dap.integration.database_errors import NonExistingTableError

region = os.environ.get('AWS_REGION')

config = Config(region_name=region)
ssm_provider = parameters.SSMProvider(config=config)

logger = Logger()

env = os.environ.get('ENV', 'dev')
db_user_secret_name = os.environ.get('DB_USER_SECRET_NAME')

param_path = f'/{env}/canvas_data_2'

api_base_url = os.environ.get('API_BASE_URL', 'https://api-gateway.instructure.com')

namespace = 'canvas'

dap.plugins.load()


def lambda_handler(event, context: LambdaContext):
    params = ssm_provider.get_multiple(param_path, max_age=600, decrypt=True)

    dap_client_id = params['dap_client_id']
    dap_client_secret = params['dap_client_secret']

    db_user_secret = parameters.get_secret(db_user_secret_name, transform="json")
    db_user = db_user_secret['username']
    db_password = quote_plus(db_user_secret['password'])
    db_name = db_user_secret['dbname']
    db_host = db_user_secret['host']
    db_port = db_user_secret['port']

    conn_str = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    credentials = Credentials.create(client_id=dap_client_id, client_secret=dap_client_secret)

    table_name = event['table_name']

    logger.info(f"syncing table: {table_name}")

    os.chdir("/tmp/")

    try:
        asyncio.get_event_loop().run_until_complete(
            sync_db(
                base_url=api_base_url,
                namespace=namespace,
                table_name=table_name,
                credentials=credentials,
                connection_string=conn_str,
            )
        )

        event['state'] = 'complete'
    except NonExistingTableError as e:
        logger.exception(e)
        event['state'] = 'needs_init'

    logger.info(f"event: {event}")

    return event
