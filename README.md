# Vietnamese Address Parser & Standardizer

This project is a data processing pipeline designed to parse, analyze, and standardize unstructured Vietnamese addresses. It takes raw, often messy, address strings as input and identifies the corresponding administrative divisions (Province/Thành phố, District/Quận/Huyện, and Ward/Phường/Xã) by matching them against official government data. A scoring algorithm is then used to infer the most probable structured address for each input.

## How It Works

The pipeline consists of three main stages:

1.  **Data Preparation (`prepare.py`)**:
    *   It reads the official list of Vietnamese administrative units from an Excel file (`dataset/Danh sách cấp xã ___25_05_2025.xls`).
    *   The data is cleaned and normalized: text is lowercased, extra spaces are removed, and prefixes like "tỉnh", "thành phố", "quận", "huyện", "xã", "phường" are separated from the names.
    *   A comprehensive set of name variants is generated for each administrative unit. This includes unaccented versions and common abbreviations to improve matching accuracy.
    *   The final, cleaned, and standardized dataset of administrative areas is saved to `dataset/param_c06_distilled.parquet`.

2.  **Address Matching (`main.py`)**:
    *   This script serves as the main entry point. It takes a list of raw addresses as input (e.g., from an Excel file).
    *   It uses the `re2` library for high-performance regular expression matching to find all possible occurrences of the prepared ward, district, and province names within each raw address string.
    *   The results of this matching phase are saved into intermediate parquet files: `ward_match.parquet`, `district_match.parquet`, and `province_match.parquet`.

3.  **Inference & Scoring (`inference.py`)**:
    *   This is the core logic for resolving ambiguities. It combines the matches from the previous step.
    *   Several scoring strategies are applied based on the completeness and the relative order of the found units. For example, an address containing a "Ward, District, Province" sequence in the correct order receives a higher score than one with just a "District" and "Province".
    *   The final output is a ranked list of the most likely standardized addresses, with the highest-scoring match selected for each input address. The results are saved to `test.xlsx` and `test.csv`.

## Project Structure

```
.
├── dataset/
│   ├── Danh sách cấp xã ___25_05_2025.xls  # Raw official administrative data
│   └── param_c06_distilled.parquet        # Prepared, standardized data (generated)
├── main.py             # Main entry point to run the full pipeline
├── prepare.py          # Cleans and prepares the official administrative data
├── model.py            # Defines data classes (Area, Ward, District, Province, etc.)
├── inference.py        # Contains the logic for scoring and inferring the best address match
├── variant.py          # (Not shown) Generates name variations for matching
├── sample.py           # Contains sample address data for testing
├── Makefile            # Convenience commands for setup and execution
├── pyproject.toml      # Project metadata and dependencies
└── README.md           # This file
```

## Setup and Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Install dependencies:**
    This project uses `uv` for package management. Ensure you have it installed. The dependencies are listed in `pyproject.toml`.
    ```bash
    uv pip install -r requirements.txt
    # or directly from the pyproject.toml
    uv pip install .
    ```
    Key dependencies include `polars`, `re2`, `openpyxl`, and `tqdm`.

## Usage

1.  **Prepare Input Data**: Place your raw addresses in an Excel file. The file should have at least two columns: a unique `ID` and the address string `ADDR`. See `sample.py` for an example format.

2.  **Update Input Path**: In `main.py`, modify the following line to point to your input file:
    ```python
    sample_addrs = normalize(pl.read_excel("./dataset/your_input_file.xlsx"))
    ```

3.  **Run the Pipeline**:
    Execute the main script from the root directory:
    ```bash
    python main.py
    ```
    Alternatively, if a `Makefile` is configured:
    ```bash
    make run
    ```

4.  **Check the Output**: The final, standardized addresses will be available in `test.xlsx` and `test.csv`.
