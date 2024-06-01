import os
import logging
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

class MinioConfig:
    def __init__(self):
        self.client = Minio(
            os.getenv('MINIO_ENDPOINT', 'minioserver:9000'),
            access_key=os.getenv('AWS_ACCESS_KEY_ID'),
            secret_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            secure=False
        )
        self.bucket = os.getenv('MINIO_BUCKET', 'mediae')

    def ensure_bucket_exists(self):
        logger.info("Ensuring MinIO bucket exists...")
        try:
            if not self.client.bucket_exists(self.bucket):
                logger.info("Bucket does not exist. Creating bucket: %s", self.bucket)
                self.client.make_bucket(self.bucket)
            else:
                logger.info("Bucket already exists: %s", self.bucket)
        except S3Error as e:
            logger.error("Failed to ensure MinIO bucket: %s", e)
            raise