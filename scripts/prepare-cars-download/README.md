# Prepare CAR Download

This folder contains the large-space CAR download tooling.

It is intentionally separate from the migration workflow. The migration system produces `state.json` with resolved shard `sourceURL` values. These scripts use that persisted inventory to prepare and execute a bulk CAR download.

## Requirements

You need a download tool that supports manifest files. This allows you to simply import the generated manifest and wait for the download to complete. This repository suggests using `aria2` and assumes you already have it installed.

### Installation

* **macOS:**

  ```
  brew install aria2
  ```

* **Linux (Debian/Ubuntu):**

  ```
  sudo apt install aria2
  ```

* **Windows:**
  1. Download the latest `.zip` file from the [official website](https://aria2.github.io/).
  2. Extract the `aria2c.exe` file and add it to your system's PATH.

## Files

* `prepare-cars-download.js`
  * reads an existing migration `state.json`
  * chooses one space
  * deduplicates shards by shard CID
  * writes the durable and runnable download artifacts
* `run-cars-download.sh`
  * launches `aria2c` against the prepared input file
* `README.md`
  * this runbook

## Inputs

The prepare script needs:

* `--state-file <path>`
* `--download-dir <path>`

The launcher needs:

* `download-dir`

## Output Layout

The output directory looks like:

```text
download-dir/
  cars/
    <shardCid>.car
  download.ndjson
  download.aria2
  summary.json
  conflicts.ndjson
  aria2.session
  .lock
```

### Artifacts

* `cars/<shardCid>.car`
  * one CAR per unique shard CID
  * flat directory layout
* `download.ndjson`
  * durable source of truth for the planned download set
* `download.aria2`
  * generated aria2 input file for the next run
* `summary.json`
  * aggregate counts and totals
* `conflicts.ndjson`
  * records files that were moved aside before redownload
* `aria2.session`
  * aria2 session file written by the launcher

## How Resume Works

Resume is coordinated by the prepare step first, not only by aria2.

On every `prepare-cars-download.js` run:

* if `cars/<shardCid>.car` is missing:
  * it is added to `download.aria2`
* if it exists and its size matches the expected shard size:
  * it is treated as complete
  * it is not added to `download.aria2`
* if it exists and its size does not match:
  * it is moved to `cars/.conflicts/`
  * a fresh download entry is added to `download.aria2`

aria2 then provides additional partial-file resume behavior through:

* `--continue`
* `aria2.session`
* per-file `.aria2` control files

This means users can interrupt the download and rerun:

1. `prepare-cars-download.js`
2. `run-cars-download.sh`

without re-downloading already complete files.

## Runbook

### 1. Prepare the download

```bash
node prepare-cars-download.js \
  --state-file /path/to/state.json \
  --download-dir /path/to/download-dir \
```

### 2. Launch aria2

```bash
bash run-cars-download.sh /path/to/download-dir
```

Optional tuning:

```bash
bash run-cars-download.sh /path/to/download-dir 24
```

Where:

* argument 2 = max concurrent files

## Notes

* downloads are deduplicated by shard CID
* `sourceURL` is used exactly as persisted in inventory
* both `shards` and `shardsToStore` are included
* skipped uploads are not downloaded; they are only counted in `summary.json`
