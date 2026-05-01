import time
from pathlib import Path
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
        cache_dir=None,
    ):
        """
        Parameters
        ----------
        cache_dir : str or Path, optional
            Local directory for caching downloaded CSVs.
            If None, caching is disabled.
        """
        if not all([access_key, secret_key, endpoint_url]):
            raise ValueError("Missing S3 credentials.")

        self.bucket = bucket
        self.folder = folder
        self.retries = max(0, int(retries))
        self.backoff_seconds = max(0.0, float(backoff_seconds))
        self.cache_dir = Path(cache_dir) if cache_dir else None

        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

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

    def _cache_path(self, filename):
        """Returns the local cache path for a filename, or None if caching is off."""
        if self.cache_dir is None:
            return None
        return self.cache_dir / filename

    def _load_from_s3(self, s3_path, filename):
        """Pull a CSV from S3 with retries. Returns a DataFrame or raises."""
        last_error = None
        for attempt in range(self.retries + 1):
            try:
                with self._filesystem.open(s3_path) as f:
                    df = pd.read_csv(f)
                return df
            except Exception as e:
                last_error = e
                if attempt < self.retries:
                    wait = self.backoff_seconds * (2**attempt)
                    print(f">> Attempt {attempt + 1} failed for {filename}: {e}. Retrying in {wait:.1f}s…")
                    time.sleep(wait)

        raise RuntimeError(
            f"Failed to load '{filename}' after {self.retries + 1} attempts: {last_error}"
        ) from last_error

    def get_csv(
        self,
        filename,
        bucket=None,
        folder: str | list[str] | None = None,
        output="pd",
        force_refresh=False,
    ):
        """
        Load a CSV file, using the local cache when available.

        Parameters
        ----------
        output : {"pd", "pl"}
        force_refresh : bool
            If True, ignore the local cache and re-download from S3,
            overwriting the cached copy.
        """
        output = (output or "pd").lower()
        if output not in {"pd", "pl"}:
            raise ValueError("output must be 'pd' or 'pl'.")

        local_path = self._cache_path(filename)
        s3_path = self._resolve_path(filename, bucket, folder)

        try:
            # --- Serve from cache ---
            if local_path and local_path.exists() and not force_refresh:
                print(f">> Cache hit: {filename} (from {local_path})")
                df = pd.read_csv(local_path)

            # --- Download from S3 ---
            else:
                if force_refresh and local_path and local_path.exists():
                    print(f">> Force refresh: {filename}")
                else:
                    print(f">> Cache miss: {filename} — downloading from S3…")

                df = self._load_from_s3(s3_path, filename)
                print(f">> Loaded: {filename}")

                if local_path:
                    df.to_csv(local_path, index=False)
                    print(f">> Cached: {filename} → {local_path}")

        except Exception as e:
            print(f">> Error loading {filename}: {e}")
            return None

        if output == "pl":
            try:
                import polars as pl
            except Exception as e:
                raise ImportError("polars is required when output='pl'.") from e
            return pl.from_pandas(df)

        return df

    def refresh(self, filename, bucket=None, folder=None, output="pd"):
        """
        Force re-download a file from S3, overwriting the local cache.

        Shorthand for get_csv(..., force_refresh=True).
        """
        return self.get_csv(filename, bucket=bucket, folder=folder, output=output, force_refresh=True)

    def clear_cache(self, filename=None):
        if not self.cache_dir:
            print(">> No cache_dir configured.")
            return

        if filename:
            path = self._cache_path(filename)
            if path and path.exists():
                path.unlink()
                print(f">> Cache cleared: {filename}")
            else:
                print(f">> Not in cache: {filename}")
        else:
            deleted = list(self.cache_dir.glob("*.csv"))
            for f in deleted:
                f.unlink()
            print(f">> Cache cleared: {len(deleted)} file(s) removed from {self.cache_dir}")

    def download(self, filename, bucket=None, folder=None, dest=None):
        """
        Download a file from S3 as-is to a local path.

        Parameters
        ----------
        filename : str
            Remote filename (e.g. 'template.jpg').
        dest : str or Path, optional
            Local destination path or directory.
            - If a directory, saves filename inside it.
            - If None, uses cache_dir if set, else current working directory.
        """
        s3_path = self._resolve_path(filename, bucket, folder)

        if dest is None:
            dest = self.cache_dir or Path.cwd()

        dest = Path(dest)
        local_path = dest / filename if dest.is_dir() else dest
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if local_path.exists():
            print(f">> Already exists: {local_path} (use force_refresh=True to overwrite)")
            return local_path

        last_error = None
        for attempt in range(self.retries + 1):
            try:
                with self._filesystem.open(s3_path, "rb") as src, local_path.open("wb") as dst:
                    dst.write(src.read())
                print(f">> Downloaded: {filename} → {local_path}")
                return local_path
            except Exception as e:
                last_error = e
                if attempt < self.retries:
                    wait = self.backoff_seconds * (2**attempt)
                    print(f">> Attempt {attempt + 1} failed for {filename}: {e}. Retrying in {wait:.1f}s…")
                    time.sleep(wait)

        print(f">> Error downloading {filename} after {self.retries + 1} attempts: {last_error}")
        return None
            