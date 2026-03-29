import pandas as pd


class S3Client:
    def __init__(
        self,
        access_key,
        secret_key,
        endpoint_url,
        bucket=None,
        folder=None,
    ):
        """
        Simple S3 client for loading lab data.

        Parameters
        ----------
        access_key : str
        secret_key : str
        endpoint_url : str
        bucket : str, optional
        folder : str, optional
        """

        if not all([access_key, secret_key, endpoint_url]):
            raise ValueError("Missing S3 credentials.")

        self.bucket = bucket
        self.folder = folder

        self.storage_options = {
            "key": access_key,
            "secret": secret_key,
            "client_kwargs": {
                "endpoint_url": endpoint_url
            }
        }

    def get_csv(self, filename, bucket=None, folder=None):
        """
        Load a CSV file from S3.

        You can override bucket/folder per call if needed.
        """

        bucket = bucket or self.bucket
        folder = folder or self.folder

        if not bucket or not folder:
            raise ValueError("Bucket and folder must be specified.")

        s3_path = f"s3://{bucket}/{folder}/{filename}"

        try:
            df = pd.read_csv(s3_path, storage_options=self.storage_options)
            print(f">> Loaded: {filename}")
            return df
        except Exception as e:
            print(f">> Error loading {filename}: {e}")
            return None