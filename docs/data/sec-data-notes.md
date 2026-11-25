# SEC insider data sets: notes for implementers

Hand-written companion to the generated [sec-schema.md](sec-schema.md). Sources: the SEC readme (`insider_transactions_readme.pdf`, also shipped inside each archive), the archives themselves, and EDGAR metadata checked on 25 Nov 2026.

## What the archives are

- One zip per calendar quarter, covering Forms 3, 4, 5 and their amendments (3/A, 4/A, 5/A) filed in XML from 2006 on. Filings received after 17:30 ET on the last business day of a quarter go into the next quarter's file.
- Eight tab-separated UTF-8 tables plus a W3C tabular-metadata JSON and an HTML readme. The metadata/readme file names changed over time (`insider_transactions_metadata.json` in 2012, `FORM_345_metadata.json` later), so files are identified by table name, not exact filename.
- Data is published "as filed": typos, inconsistent encodings and redundancies relative to earlier filings are expected.
- Links come from the data sets page. In 2026 the newest quarters moved from `/files/structureddata/...` to `/files/datastandardsinnovation/...`, and older quarters are occasionally republished (2023Q3 was last modified in July 2025), so archives are versioned by SHA-256.

## Keys and joins

| Table | Key | Joins to |
|---|---|---|
| SUBMISSION | `ACCESSION_NUMBER` | everything |
| REPORTINGOWNER | `ACCESSION_NUMBER`, `RPTOWNERCIK` | SUBMISSION (one filing can have several owners) |
| NONDERIV_TRANS / DERIV_TRANS | `ACCESSION_NUMBER`, `*_SK` | SUBMISSION; footnotes via the `*_FN` columns |
| NONDERIV_HOLDING / DERIV_HOLDING | `ACCESSION_NUMBER`, `*_SK` | SUBMISSION |
| FOOTNOTES | `ACCESSION_NUMBER`, `FOOTNOTE_ID` | any `*_FN` column (comma-separated ids such as `F1,F2`) |

Transaction rows belong to the filing, not to an individual reporting owner. On multi-owner filings there is no field saying which owner made which transaction.

## Fields that matter and their quirks

- **Dates** are `DD-MON-YYYY` (e.g. `02-JAN-2019`). Transaction dates include obvious typos (years in the future or decades in the past).
- **`FILING_DATE`** is the only time field. There is **no acceptance timestamp**; it comes from EDGAR submissions metadata (PR 5). Forms 3/4/5 submitted up to 22:00 ET get that day's filing date.
- **`DATE_OF_ORIG_SUB`** is set on amendments and gives the original filing's date. There is no "original accession number" field, so amendments are linked heuristically.
- **`AFF10B5ONE`** (Rule 10b5-1 checkbox) appears in SUBMISSION from 2023 on, so it is **filing-level**. Values seen: `0`, `1`, `false`, `true`, empty. Empty means unknown, never "discretionary".
- **`ISSUERTRADINGSYMBOL`** is the symbol as reported at filing time, with placeholders such as `NONE`, `N/A` and foreign listings. It is a useful free history of tickers by date.
- **`RPTOWNER_RELATIONSHIP`** is a comma-separated list (`Director,Officer`). `RPTOWNER_TITLE` is free text (`See Remarks`, `President & CEO`, …).
- **`TRANS_CODE`**: P/S are "open market *or private*" purchases/sales. ESPP, dividend reinvestment, offerings and private placements show up under P and are separated by footnote rules.
- **`TRANS_PRICEPERSHARE`** is often a weighted average, with the range given in a footnote. It is zero or missing for gifts, grants and some transfers.
- **`SHRS_OWND_FOLWNG_TRANS`** is the holding after the line, per ownership form (direct vs each indirect entity), so lines must be grouped by ownership form to rebuild prior holdings.
