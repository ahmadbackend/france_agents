# Web Scraping Agents Collection

This repository contains a collection of web scrapers designed to extract real estate agent information from multiple websites. Each website is organized into its own directory with dedicated scripts for scraping and optional post-processing.

## Project Structure

* Each **website** has its own **directory**.
* Inside each directory you will typically find:

  * A **main scraping script** responsible for collecting the raw data.
  * An optional **post‑processing script** named `*_clean.py`.

### Post‑Processing Scripts

If present, the `*_clean.py` script should be executed **after the scraping process finishes**.
These scripts are used to perform additional data cleaning and transformations such as:

* Removing unnecessary attributes
* Generating profile URLs
* Decoding phone numbers
* Normalizing extracted data
* Other dataset‑specific cleanup tasks

## Dependencies

All required dependencies are listed in `requirements.txt`.

Install them using:

```
python -m pip install -r requirements.txt
```

## Python Version

This project was developed and tested using:

**Python 3.11**

Using the same version is recommended to ensure compatibility.

## Special Case: IAD Website

The scraping workflow for the **IAD** website is divided into two steps:

### 1. Generate Location URLs

Run:

```
iad_location_generator.py
```

This script generates all possible location URLs used to discover agents.

### 2. Scrape Agents

Run:

```
iad_agents.py
```

This script collects agent data from all previously generated location URLs.

## General Workflow

1. Navigate to the directory of the target website.
2. Run the main scraping script to collect raw data.
3. If available, execute the corresponding `*_clean.py` script to clean and normalize the results.

## Notes

* Each scraper is designed specifically for the structure of its target website.
* Website changes may require updating selectors or extraction logic.
* Post‑processing scripts are optional and only exist when additional data cleaning is required.
