# ipynb-jjsm-tools

Utility tools for Jupyter notebooks and data workflows.

Designed for reusable, clean, and minimal setup across multiple notebooks without duplicating code.

---

## Features

- Easy data loading from S3-compatible storage  
- Notebook-friendly API (no repeated setup)  
- Supports environment-based secrets (.env)  
- Install directly from GitHub (no PyPI needed)  

---

## Installation

### From GitHub
pip install git+https://github.com/YOUR_USER/ipynb-jjsm-tools.git

### With uv (recommended)
uv pip install git+https://github.com/YOUR_USER/ipynb-jjsm-tools.git

---

## Project Structure

ipynb_jjsm_tools/
└── download/
    └── S3Client

---

## Configuration (Secrets)

Create a `.env` file:

S3_ACCESS_KEY=your_key  
S3_SECRET_KEY=your_secret  
S3_ENDPOINT_URL=https://your-endpoint  

Make sure to add this to `.gitignore`:

.env

---

## Usage

### Basic example

from ipynb_jjsm_tools.download import S3Client

s3 = S3Client(
    bucket="uc-courses",
    folder="fiz121l"
)

df = s3.get_csv("l1_exp_02.csv")

---

### With explicit credentials

s3 = S3Client(
    access_key="YOUR_KEY",
    secret_key="YOUR_SECRET",
    endpoint_url="https://your-endpoint",
    bucket="uc-courses",
    folder="fiz121l"
)

---

### Override per call

df = s3.get_csv("file.csv", folder="fiz221l")

---

## Design Philosophy

- Keep notebooks clean and minimal
- Avoid repeated boilerplate
- Make tools feel like a personal standard library
- Prefer explicit configuration over hidden state

---

## Recommended Setup

Use a single shared environment for all notebooks:

uv venv ~/.venvs/ipynb  
source ~/.venvs/ipynb/bin/activate  
uv pip install jupyter ipykernel  

Register kernel:

python -m ipykernel install --user --name ipynb --display-name "Python (ipynb)"

---

## Development

Clone the repo and install in editable mode:

git clone https://github.com/YOUR_USER/ipynb-jjsm-tools.git  
cd ipynb-jjsm-tools  
uv pip install -e .

---

## Roadmap

- Caching layer for downloaded data  
- Parquet support  
- Plotting utilities  
- Config profiles (multiple S3 environments)  

---

## License

MIT (or your preferred license)

---

## Author

Juan Sánchez