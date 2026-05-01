import time

import fsspec
import pandas as pd


class S3Client:
    def __init__(
        self,
        access_key,
        secret_key,
        endpoint_url,
        bucket=None,
        folder=None,
        retries=3,
        backoff_seconds=0.5,
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

        self.retries = max(0, int(retries))
        self.backoff_seconds = max(0.0, float(backoff_seconds))

        self.storage_options = {
            "key": access_key,
            "secret": secret_key,
            "client_kwargs": {"endpoint_url": endpoint_url},
        }

        self._filesystem = None

    def get_csv(
        self, filename, bucket=None, folder: str | list[str] | None = None, output="pd"
    ):
        """
        Load a CSV file from S3.

        You can override bucket/folder per call if needed.
        output : {"pd", "pl"}
        """

        bucket = bucket or self.bucket
        folder = folder or self.folder

        if isinstance(folder, list):
            folder = "/".join(part for part in folder if part)

        if not bucket or not folder:
            raise ValueError("Bucket and folder must be specified.")

        s3_path = f"s3://{bucket}/{folder}/{filename}"

        if self._filesystem is None:
            # Warm up the S3 filesystem connection to avoid first-call failures.
            try:
                self._filesystem = fsspec.filesystem("s3", **self.storage_options)
                self._filesystem.invalidate_cache()
            except Exception:
                self._filesystem = None

        output = (output or "pd").lower()
        if output not in {"pd", "pl"}:
            raise ValueError("output must be 'pd' or 'pl'.")

        attempt = 0
        last_error = None
        while attempt <= self.retries:
            try:
                df = pd.read_csv(s3_path, storage_options=self.storage_options)
                print(f">> Loaded: {filename}")
                if output == "pd":
                    return df
                try:
                    import polars as pl
                except Exception as e:
                    raise ImportError("polars is required when output='pl'.") from e
                return pl.from_pandas(df)
            except Exception as e:
                last_error = e
                if attempt >= self.retries:
                    break
                time.sleep(self.backoff_seconds * (2**attempt))
                attempt += 1

        print(f">> Error loading {filename}: {last_error}")
        return None
