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
        if not all([access_key, secret_key, endpoint_url]):
            raise ValueError("Missing S3 credentials.")

        self.bucket = bucket
        self.folder = folder
        self.retries = max(0, int(retries))
        self.backoff_seconds = max(0.0, float(backoff_seconds))

        self._storage_options = {
            "key": access_key,
            "secret": secret_key,
            "client_kwargs": {"endpoint_url": endpoint_url},
        }

        self._filesystem = fsspec.filesystem("s3", **self._storage_options)
        self._filesystem.invalidate_cache()

    def _resolve_path(self, filename, bucket, folder):
        bucket = bucket or self.bucket
        folder = folder or self.folder

        if isinstance(folder, list):
            folder = "/".join(part for part in folder if part)

        if not bucket or not folder:
            raise ValueError("Bucket and folder must be specified.")

        return f"s3://{bucket}/{folder}/{filename}"

    def get_csv(
        self, filename, bucket=None, folder: str | list[str] | None = None, output="pd"
    ):
        """
        Load a CSV file from S3.

        Parameters
        ----------
        output : {"pd", "pl"}
        """
        output = (output or "pd").lower()
        if output not in {"pd", "pl"}:
            raise ValueError("output must be 'pd' or 'pl'.")

        s3_path = self._resolve_path(filename, bucket, folder)

        last_error = None
        for attempt in range(self.retries + 1):
            try:
                with self._filesystem.open(s3_path) as f:
                    df = pd.read_csv(f)

                print(f">> Loaded: {filename}")

                if output == "pl":
                    try:
                        import polars as pl
                    except Exception as e:
                        raise ImportError("polars is required when output='pl'.") from e
                    return pl.from_pandas(df)

                return df

            except Exception as e:
                last_error = e
                if attempt < self.retries:
                    wait = self.backoff_seconds * (2**attempt)
                    print(
                        f">> Attempt {attempt + 1} failed for {filename}: {e}. Retrying in {wait:.1f}s…"
                    )
                    time.sleep(wait)

        print(
            f">> Error loading {filename} after {self.retries + 1} attempts: {last_error}"
        )
        return None
