# MapReduce Analysis — Amazon Health & Household Reviews

A Python MapReduce project using **MRJob** to analyse Amazon product reviews across six analytical tasks, from dataset profiling to sentiment analysis.

---

## Table of Contents

- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Setup](#setup)
- [Dataset](#dataset)
- [Running the Jobs](#running-the-jobs)
  - [Job 1 — Dataset Structure Examination](#job-1--dataset-structure-examination)
  - [Job 2 — Number of Reviews per Product](#job-2--number-of-reviews-per-product)
  - [Job 3 — Average Star Rating per Product](#job-3--average-star-rating-per-product)
  - [Job 4 — Top Ten Most-Reviewed Products](#job-4--top-ten-most-reviewed-products)
  - [Job 5 — Average Helpfulness Score](#job-5--average-helpfulness-score)
  - [Job 6 — Sentiment Analysis](#job-6--sentiment-analysis)
- [Output Files](#output-files)
- [Troubleshooting](#troubleshooting)

---

## Project Structure

```
map-reduce-project/
│
├── Health_and_Household.jsonl        # Dataset (Amazon product reviews)
│
├── job1_examine_dataset.py           # Job 1: Dataset structure profiling
├── job2_reviews_per_product.py       # Job 2: Review count per product
├── job3_average_rating.py            # Job 3: Average star rating per product
├── job4_top_ten_products.py          # Job 4: Top 10 most-reviewed products
├── job5_helpfulness_score.py         # Job 5: Average helpfulness score
├── job6_sentiment_analysis.py        # Job 6: Sentiment analysis of review text
│
│
├── requirements.txt                  # All required Python packages
│
├── job1_results.txt                  # Output: Job 1 results (auto-generated)
├── job2_results.txt                  # Output: Job 2 results (auto-generated)
├── job3_results.txt                  # Output: Job 3 results (auto-generated)
├── job4_results.txt                  # Output: Job 4 results (auto-generated)
├── job5_results.txt                  # Output: Job 5 results (auto-generated)
├── job6_results.txt                  # Output: Job 6 results (auto-generated)
├── job6_sentiment_chart.png          # Output: Sentiment chart (auto-generated)
```

---

## Requirements

### Python Version

Python **3.8 or higher** is required.

### Required Packages

| Package          | Version   | Purpose                                      |
|------------------|-----------|----------------------------------------------|
| `mrjob`          | 0.7.4     | MapReduce framework (core dependency)        |
| `matplotlib`     | ≥ 3.5     | Sentiment chart visualisation (Job 6)        |
| `numpy`          | ≥ 1.21    | Numerical support for matplotlib             |
| `Pillow`         | ≥ 9.0     | Image processing support for matplotlib      |
| `PyYAML`         | ≥ 6.0     | MRJob configuration parsing                  |
| `python-docx`    | ≥ 1.1     | Word document generation                     |
| `python-pptx`    | ≥ 1.0     | PowerPoint generation                        |
| `lxml`           | ≥ 4.9     | XML backend for python-docx                  |
| `XlsxWriter`     | ≥ 3.0     | Required by python-pptx                      |
| `contourpy`      | ≥ 1.0     | Matplotlib contour plots                     |
| `cycler`         | ≥ 0.11    | Matplotlib style cycling                     |
| `fonttools`      | ≥ 4.0     | Font handling for matplotlib                 |
| `kiwisolver`     | ≥ 1.4     | Constraint solver for matplotlib layouts     |
| `pyparsing`      | ≥ 3.0     | Parsing utilities for matplotlib             |
| `python-dateutil`| ≥ 2.8     | Date utilities                               |
| `six`            | ≥ 1.16    | Python 2/3 compatibility (mrjob dependency)  |
| `packaging`      | ≥ 21.0    | Package version handling                     |

All packages are listed in `requirements.txt`.

---

## Setup

### 1. Clone or Download the Project

Place the project folder anywhere on your machine. Make sure `Health_and_Household.jsonl` is in the project root directory.

### 2. Create a Virtual Environment

It is strongly recommended to use a virtual environment to avoid package conflicts.

**Windows (PowerShell):**
```powershell
cd path\to\map-reduce-project
python -m venv venv
```

**macOS / Linux:**
```bash
cd path/to/map-reduce-project
python3 -m venv venv
```

### 3. Activate the Virtual Environment

**Windows (PowerShell):**
```powershell
.\venv\Scripts\Activate.ps1
```

> If you get a script execution policy error on Windows, run:
> ```powershell
> Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
> ```
> Then try activating again.

**Windows (Command Prompt):**
```cmd
venv\Scripts\activate.bat
```

**macOS / Linux:**
```bash
source venv/bin/activate
```

You should see `(venv)` at the start of your terminal prompt when activated.

### 4. Install All Dependencies

```bash
pip install -r requirements.txt
```

This installs all required packages in one step. The full `requirements.txt` is:

```
mrjob==0.7.4
matplotlib>=3.5.0
numpy>=1.21.0
Pillow>=9.0.0
PyYAML>=6.0
python-docx>=1.1.0
python-pptx>=1.0.0
lxml>=4.9.0
XlsxWriter>=3.0.0
contourpy>=1.0.0
cycler>=0.11.0
fonttools>=4.0.0
kiwisolver>=1.4.0
pyparsing>=3.0.0
python-dateutil>=2.8.0
six>=1.16.0
packaging>=21.0
```

To verify installation:
```bash
python -c "import mrjob, matplotlib, docx, pptx; print('All packages installed successfully')"
```

---

## Dataset

### Source

The dataset is part of the **Amazon Reviews 2023** collection released by the McAuley Lab at UC San Diego.

| Source | Link |
|--------|------|
| Official project page | [amazon-reviews-2023.github.io](https://amazon-reviews-2023.github.io/) |
| Hugging Face (primary download) | [McAuley-Lab/Amazon-Reviews-2023](https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023) |
| Kaggle mirror | [Amazon Reviews Data 2023](https://www.kaggle.com/datasets/wajahat1064/amazon-reviews-data-2023) |

**Downloading from Hugging Face:**

Install the Hugging Face `datasets` library and download the Health & Household category directly:

```bash
pip install datasets
```

```python
from datasets import load_dataset

dataset = load_dataset(
    "McAuley-Lab/Amazon-Reviews-2023",
    "raw_review_Health_and_Household",
    trust_remote_code=True
)
```

Or download the raw JSONL file manually from the Hugging Face repository under:
`raw/review_categories/Health_and_Household.jsonl`

Place the downloaded file in the project root directory as `Health_and_Household.jsonl`.

---

### Schema

The dataset `Health_and_Household.jsonl` must be present in the project root directory. Each line is a JSON object representing one Amazon product review with the following fields:

| Field               | Type    | Description                              |
|---------------------|---------|------------------------------------------|
| `asin`              | string  | Product ID (Amazon Standard ID Number)   |
| `parent_asin`       | string  | Parent product identifier                |
| `user_id`           | string  | Reviewer user ID                         |
| `rating`            | float   | Star rating (1.0 – 5.0)                  |
| `title`             | string  | Review title                             |
| `text`              | string  | Full review body text                    |
| `timestamp`         | integer | Unix epoch timestamp in milliseconds     |
| `helpful_vote`      | integer | Number of users who marked review helpful|
| `verified_purchase` | bool    | Whether the reviewer verified purchase   |
| `images`            | array   | List of image objects attached to review |

---

## Running the Jobs

All jobs are run from the project root directory with the virtual environment **activated**. Each job reads `Health_and_Household.jsonl` by default and saves its results to a corresponding `jobN_results.txt` file.

Every job supports an optional `--sample N` flag to process only the first N records — useful for quick testing before running on the full dataset.

---

### Job 1 — Dataset Structure Examination

Profiles the dataset: field completeness, rating distribution, verified purchase breakdown, image stats, text length, date range, and helpful vote distribution.

**Full dataset:**
```bash
python job1_examine_dataset.py
```

**Sample (first 50,000 records — recommended for testing):**
```bash
python job1_examine_dataset.py --sample 50000
```

**Output:** `job1_results.txt`

---

### Job 2 — Number of Reviews per Product

Counts total reviews per product (ASIN), shows distribution buckets, and lists the top 20 most-reviewed and bottom 10 least-reviewed products.

**Full dataset:**
```bash
python job2_reviews_per_product.py
```

**Sample:**
```bash
python job2_reviews_per_product.py --sample 50000
```

**Output:** `job2_results.txt`

---

### Job 3 — Average Star Rating per Product

Computes the weighted average star rating for each product using a combiner for efficiency. Ranks products into quality tiers and lists top 20 / bottom 20 products with at least 10 reviews.

**Full dataset:**
```bash
python job3_average_rating.py
```

**Sample:**
```bash
python job3_average_rating.py --sample 50000
```

**Output:** `job3_results.txt`

---

### Job 4 — Top Ten Most-Reviewed Products

Uses a two-step MapReduce pipeline to identify the ten most-reviewed products globally and includes a gap analysis between ranks.

**Full dataset:**
```bash
python job4_top_ten_products.py
```

**Sample:**
```bash
python job4_top_ten_products.py --sample 50000
```

**Output:** `job4_results.txt`

---

### Job 5 — Average Helpfulness Score

Calculates average helpful vote counts per review, broken down by star rating, vote-count bucket, and purchase verification status.

**Full dataset:**
```bash
python job5_helpfulness_score.py
```

**Sample:**
```bash
python job5_helpfulness_score.py --sample 50000
```

**Output:** `job5_results.txt`

---

### Job 6 — Sentiment Analysis

Classifies each review as POSITIVE, NEUTRAL, or NEGATIVE using keyword scoring with a star-rating tiebreak. Produces a cross-tabulation by star rating and purchase verification, and saves a matplotlib chart.

**Full dataset:**
```bash
python job6_sentiment_analysis.py
```

**Sample:**
```bash
python job6_sentiment_analysis.py --sample 50000
```

**Output:** `job6_results.txt`, `job6_sentiment_chart.png`

---

### Generate Report Documents

Generates the full project report as a Word document and a PowerPoint presentation. **Run all six jobs first** so the result files exist.

```bash
python generate_report_docs.py
```

**Output:** `MapReduce_Project_Report.docx`, `MapReduce_Project_Report.pptx`

---

## Running All Jobs in Sequence

To run the full pipeline from start to finish:

**Windows (PowerShell):**
```powershell
python job1_examine_dataset.py
python job2_reviews_per_product.py
python job3_average_rating.py
python job4_top_ten_products.py
python job5_helpfulness_score.py
python job6_sentiment_analysis.py
```

**macOS / Linux (single command):**
```bash
python job1_examine_dataset.py && \
python job2_reviews_per_product.py && \
python job3_average_rating.py && \
python job4_top_ten_products.py && \
python job5_helpfulness_score.py && \
python job6_sentiment_analysis.py && \
```

> Note: Running all jobs on the full dataset may take several minutes depending on your machine. Use `--sample 50000` for a quick test run of each job.

---

## Output Files

| File                           | Generated by               | Description                            |
|--------------------------------|----------------------------|----------------------------------------|
| `job1_results.txt`             | `job1_examine_dataset.py`  | Dataset profiling statistics           |
| `job2_results.txt`             | `job2_reviews_per_product.py` | Review counts per product           |
| `job3_results.txt`             | `job3_average_rating.py`   | Average ratings per product            |
| `job4_results.txt`             | `job4_top_ten_products.py` | Top 10 most-reviewed products          |
| `job5_results.txt`             | `job5_helpfulness_score.py`| Helpfulness score statistics           |
| `job6_results.txt`             | `job6_sentiment_analysis.py` | Sentiment distribution results       |
| `job6_sentiment_chart.png`     | `job6_sentiment_analysis.py` | Pie + bar chart (matplotlib)         |

---

## Troubleshooting

**`ModuleNotFoundError: No module named 'mrjob'`**
> The virtual environment is not activated, or dependencies were not installed. Activate the venv and run `pip install -r requirements.txt`.

**`FileNotFoundError: Health_and_Household.jsonl`**
> The dataset file must be in the same directory as the job scripts. Check the filename matches exactly (case-sensitive on Linux/macOS).

**`Set-ExecutionPolicy` error on Windows**
> PowerShell script execution is disabled. Run this once as Administrator:
> ```powershell
> Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
> ```

**`matplotlib not installed` warning in Job 6**
> The sentiment chart will not be generated but text results will still be saved. Install matplotlib with `pip install matplotlib`.

**Jobs run slowly on the full dataset**
> MRJob's inline runner runs in a single Python process. Use `--sample 50000` for faster testing. For large-scale execution, configure MRJob to run on Hadoop or EMR by changing `-r inline` to `-r hadoop` or `-r emr` in the job scripts.

**`UnicodeEncodeError` on Windows console**
> All scripts call `sys.stdout.reconfigure(encoding="utf-8")` at startup. If you still see errors, run:
> ```powershell
> $env:PYTHONIOENCODING = "utf-8"
> ```
> before executing the scripts.
