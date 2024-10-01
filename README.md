# Overview
This tool will ingest saved objects (formatted as a record) directly into a DDB repository. This is useful for just populating saved objects from a template quickly. 

The saved objects stored in DDB will be slightly different than OpenSearch Dashboards saved objects. They follow the format:
```json
{
  "applicationId": "SOME-APP-2",
  "id": "<saved object type>:",
  "attributes": {
    // Saved objects attributes
  },
  "migrationVersion": {
    // Migration version
  },
  "references": [
    // Saved object references
  ],
  "type": "<saved object type>",
  "updated_at": "<timestamp>",
  "version": 1
}
```

**Note**: this tool will make direct DDB calls. Use this tool in your personal testing environment where possible.

## Setup
1. Install requirements
```bash
pip install -r requirements.txt
```

## Usage
```bash
python ingest_sample_saved_objects.py [num_records] [saved_objects_file_path] [replace_timestamp] [replace_title]
```

- **num_records**: How many times to ingest a saved object or a batch of saved objects. Default is `10`
- **saved_objects_file_path**: The file containing the saved objects template. Can be a `json` or `ndjson` file (if a `json` file, only one saved object can be specified). Default is `sample_saved_objects.ndjson`
- **replace_timestamp**: Every saved object has an `updated_at` field. This option can toggle whether to replace the value with the current timestamp. Default is `true`
- **replace_title**: Some saved objects have a title. This option can toggle whether to replace the title with a random title. Default is `true`