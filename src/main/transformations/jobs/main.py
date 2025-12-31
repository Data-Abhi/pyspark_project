from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *

aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(aws_access_key, aws_secret_key)
s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()
logger.info("list of buckets: %s", response['Buckets'])