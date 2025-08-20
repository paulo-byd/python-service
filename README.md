# Silver PDF DMS

A PDF processing and document management system for BYD Brazil Systems Team.

## What it does

1. Downloads PDF files from the DMS System
2. Processes PDFs to extract information (using AI/ML or API)
3. Stores results in the database
4. Matches processed data with DMS data for validation

## Installation

### 1. Install Python Requirements
```bash
pip install -r requirements.txt
```

### 2. Setup Database
Run the SQL script to create required tables:
```bash
# Connect to your BGATE database and execute:
sqlplus username/password@database
@scripts.sql
```

### 3. Configure the System
Copy and edit the configuration file:
```bash
cp config.yaml.example config.yaml
# Edit config.yaml with your settings
```

## Running the System

### Main Service
```bash
# Run once
python pdf_dms_main.py --run-once --environment local

# Run continuously
python pdf_dms_main.py --environment local
```

### Test Pipeline
```bash
# Test processing and matching
python test_pipeline.py --claims 5 --test-type full --environment local
```

### API Service
```bash
python cat_api.py --env local
```

## Configuration

Here's an example `config.yaml` with explanations:

```yaml
# Database connections for different environments
databases:
  local:
    dms:
      user: "DMS_OEM_SL"                    # DMS database username
      password: "your-password"              # DMS database password
      dsn: "host:port/service"               # DMS database connection string
    bgate:
      user: "temp_dms"                       # BGATE database username
      password: "your-password"              # BGATE database password
      dsn: "host:port/service"               # BGATE database connection string

# How often to run the pipeline (in minutes)
scheduling:
  enabled: true                              # Enable automatic scheduling
  interval_minutes: 60                       # Run every 60 minutes
  run_on_startup: true                       # Run immediately when starting

# Where to store downloaded PDF files
download:
  storage_path: "./pdf-claims"               # Local storage directory
  batch_size: 20                             # Process 20 claims at a time
  max_retries: 3                             # Retry failed downloads 3 times

# PDF Processing settings
pdf_processing:
  max_claims_per_batch: 10                   # Process 10 claims per batch
  
  # Choose between local processing and API processing
  use_api_processing: false                  # false = local, true = API
  
  # API settings (only used if use_api_processing = true)
  api:
    base_url: "https://your-api-server.com"  # API server URL
    endpoint: "/process-pdfs"                 # API endpoint
    timeout_seconds: 300                     # API timeout (5 minutes)
    api_key: "your-api-key"                  # API authentication key

# Invoice matching settings
matching:
  max_claims_per_batch: 50                   # Match 50 claims per batch
  exact_amount_match: true                   # Require exact amount matching
  tolerance_percentage: 0.0                  # Allow 0% difference in amounts

# Logging settings
logging:
  level: "INFO"                              # Log level: DEBUG, INFO, WARNING, ERROR
  file_path: "pdf_dms_service.log"           # Log file name

# Development/testing options
development:
  use_mock_processing: false                 # Use fake data for testing
  mock_success_rate: 0.7                     # 70% success rate for mock data
```

## Environment Options

- `local`: Development environment
- `uat`: Testing environment  
- `prod`: Production environment

## Quick Start

1. **Install**: `pip install -r requirements.txt`
2. **Setup DB**: Run `scripts.sql` in your database
3. **Configure**: Edit `config.yaml` with your database details
4. **Test**: `python test_pipeline.py --claims 1 --environment local`
5. **Run**: `python pdf_dms_main.py --environment local`

## File Structure

```
silver-pdf-dms/
├── pdf_dms_main.py          # Main service
├── downloader.py            # Downloads PDFs
├── pdf_processing.py        # Processes PDFs
├── matching_invoices.py     # Matches data
├── database_ops.py          # Database operations
├── test_pipeline.py         # Testing tool
├── config.yaml              # Configuration
└── requirements.txt         # Python packages
```