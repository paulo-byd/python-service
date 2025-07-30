#!/bin/bash

# PDF Download Service Startup Script
# This script helps start the service with proper environment setup

set -e

# Configuration
SERVICE_NAME="PDF Download Service"
PYTHON_SCRIPT="main.py"
CONFIG_FILE="config.yaml"
VENV_DIR="venv"
LOG_FILE="startup.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
  echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" >>"$LOG_FILE"
}

warn() {
  echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1" >>"$LOG_FILE"
}

error() {
  echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1" >>"$LOG_FILE"
  exit 1
}

info() {
  echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $1${NC}"
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $1" >>"$LOG_FILE"
}

# Function to check if a command exists
command_exists() {
  command -v "$1" >/dev/null 2>&1
}

# Function to check Python version
check_python_version() {
  if command_exists python3; then
    PYTHON_CMD="python3"
  elif command_exists python; then
    PYTHON_CMD="python"
  else
    error "Python is not installed or not in PATH"
  fi

  PYTHON_VERSION=$($PYTHON_CMD --version 2>&1 | grep -oE '[0-9]+\.[0-9]+')
  PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
  PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

  if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 8 ]); then
    error "Python 3.8 or higher is required. Found: $PYTHON_VERSION"
  fi

  log "Python version check passed: $PYTHON_VERSION"
}

# Function to setup virtual environment
setup_venv() {
  if [ ! -d "$VENV_DIR" ]; then
    log "Creating Python virtual environment..."
    $PYTHON_CMD -m venv "$VENV_DIR"
  else
    log "Virtual environment already exists"
  fi

  # Activate virtual environment
  source "$VENV_DIR/bin/activate"

  # Upgrade pip
  pip install --upgrade pip

  # Install requirements
  if [ -f "requirements.txt" ]; then
    log "Installing Python dependencies..."
    pip install -r requirements.txt
  else
    warn "requirements.txt not found, installing basic dependencies..."
    pip install requests PyYAML APScheduler pandas oracledb
  fi
}

# Function to validate configuration
validate_config() {
  if [ ! -f "$CONFIG_FILE" ]; then
    error "Configuration file $CONFIG_FILE not found"
  fi

  log "Configuration file found: $CONFIG_FILE"

  # Check if config is valid YAML
  if command_exists python3; then
    python3 -c "import yaml; yaml.safe_load(open('$CONFIG_FILE'))" 2>/dev/null || error "Invalid YAML in configuration file"
  fi

  log "Configuration validation passed"
}

# Function to check database connectivity
check_database() {
  log "Checking database connectivity..."

  if [ -f "db_handler.py" ]; then
    python3 -c "
import db_handler
try:
    config = db_handler.load_config()
    conn = db_handler.get_db_connection()
    if conn:
        conn.close()
        print('Database connection successful')
        exit(0)
    else:
        print('Database connection failed')
        exit(1)
except Exception as e:
    print(f'Database check failed: {e}')
    exit(1)
" || error "Database connectivity check failed"
  else
    warn "db_handler.py not found, skipping database check"
  fi

  log "Database connectivity check passed"
}

# Function to check required files
check_files() {
  log "Checking required files..."

  required_files=("$PYTHON_SCRIPT" "db_handler.py")

  for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
      error "Required file not found: $file"
    fi
  done

  log "All required files found"
}

# Function to check storage directory
check_storage() {
  log "Checking storage directory..."

  # Extract storage path from config
  if command_exists python3; then
    STORAGE_PATH=$(python3 -c "
import yaml
with open('$CONFIG_FILE', 'r') as f:
    config = yaml.safe_load(f)
env = config.get('environment', 'local')
overrides = config.get('environment_overrides', {}).get(env, {})
storage_path = overrides.get('download', {}).get('storage_path') or config.get('download', {}).get('storage_path')
print(storage_path if storage_path else '')
" 2>/dev/null)

    if [ -n "$STORAGE_PATH" ]; then
      if [ ! -d "$STORAGE_PATH" ]; then
        log "Creating storage directory: $STORAGE_PATH"
        mkdir -p "$STORAGE_PATH" || error "Failed to create storage directory: $STORAGE_PATH"
      fi
      log "Storage directory ready: $STORAGE_PATH"
    else
      warn "Could not determine storage path from configuration"
    fi
  fi
}

# Function to show service information
show_info() {
  info "=== $SERVICE_NAME Startup Information ==="
  info "Python Command: $PYTHON_CMD"
  info "Python Version: $PYTHON_VERSION"
  info "Configuration: $CONFIG_FILE"
  info "Virtual Environment: $VENV_DIR"
  info "Main Script: $PYTHON_SCRIPT"
  info "Log File: $LOG_FILE"
  info "Working Directory: $(pwd)"
  info "User: $(whoami)"
  info "Date: $(date)"
  info "======================================="
}

# Function to start the service
start_service() {
  log "Starting $SERVICE_NAME..."

  # Activate virtual environment
  source "$VENV_DIR/bin/activate"

  # Run the service
  exec $PYTHON_CMD "$PYTHON_SCRIPT"
}

# Main execution
main() {
  # Parse command line arguments
  RUN_ONCE=false
  SKIP_CHECKS=false

  while [[ $# -gt 0 ]]; do
    case $1 in
    --run-once)
      RUN_ONCE=true
      shift
      ;;
    --skip-checks)
      SKIP_CHECKS=true
      shift
      ;;
    --help | -h)
      echo "Usage: $0 [OPTIONS]"
      echo "Options:"
      echo "  --run-once     Run service once and exit"
      echo "  --skip-checks  Skip pre-flight checks"
      echo "  --help, -h     Show this help message"
      exit 0
      ;;
    *)
      error "Unknown option: $1"
      ;;
    esac
  done

  # Show service information
  show_info

  if [ "$SKIP_CHECKS" = false ]; then
    # Run pre-flight checks
    log "Running pre-flight checks..."
    check_python_version
    check_files
    validate_config
    setup_venv
    check_storage
    check_database
    log "All pre-flight checks passed"
  else
    # Still need to setup venv
    setup_venv
    warn "Pre-flight checks skipped"
  fi

  # Start the service
  if [ "$RUN_ONCE" = true ]; then
    log "Starting service in run-once mode..."
    source "$VENV_DIR/bin/activate"
    $PYTHON_CMD "$PYTHON_SCRIPT" --run-once
  else
    start_service
  fi
}

# Handle signals for graceful shutdown
trap 'log "Received shutdown signal, stopping service..."; exit 0' SIGTERM SIGINT

# Run main function
main "$@"
