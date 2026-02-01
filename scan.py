import requests
import re
import csv
from tqdm import tqdm
from pathlib import Path

API_URL = "https://commons.wikimedia.org/w/api.php"
files_path = Path(__file__).parent / "files.txt"
# Read file names from files.txt
with open(files_path, "r", encoding="utf-8") as f:
    files = [line.strip() for line in f if line.strip()]

rows = []

for filename in tqdm(files):
    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "titles": f"File:{filename}",
        "rvprop": "content",
        "rvslots": "main",
        "formatversion": 2,
    }

    try:
        response = requests.get(API_URL, params=params, timeout=15)
        response.raise_for_status()
    except Exception as e:
        rows.append([filename, "", f"Request error: {e}"])
        continue

    data = response.json()
    pages = data.get("query", {}).get("pages", [])

    if not pages or "revisions" not in pages[0]:
        rows.append([filename, "", "No revisions found"])
        continue

    # Extract raw wikitext
    wikitext = pages[0]["revisions"][0]["slots"]["main"]["content"]

    # Search for |source = URL
    match = re.search(
        r"\|\s*source\s*=\s*(https?://[^\s|]+)",
        wikitext,
        flags=re.IGNORECASE,
    )

    source_url = match.group(1) if match else ""
    rows.append([filename, source_url, ""])

results_path = Path(__file__).parent / "results.csv"
# Write results to CSV file
with open(results_path, "w", encoding="utf-8", newline="") as out:
    writer = csv.writer(out)
    writer.writerow(["filename", "source_url", "error"])
    writer.writerows(rows)
