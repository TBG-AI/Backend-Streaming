#!/bin/bash
# tests/dump_database.sh
set -e

# Default values
ENV_FILE=".env.local"
DUMP_FILE="./tests/db_schema.sql"
DUMP_DIR="./tests"
SCHEMA_ONLY=false

# Tables in proper dependency order to prevent foreign key violations
ORDERED_TABLES=(
  # Base tables based on models.py
  "teams"
  "players"
)

# Default is all tables in proper order
DEFAULT_TABLES=$(IFS=,; echo "${ORDERED_TABLES[*]}")
TABLES="$DEFAULT_TABLES"

# Process command line arguments
while getopts "e:o:st:h" opt; do
  case $opt in
    e) ENV_FILE="$OPTARG" ;;
    o) DUMP_FILE="$OPTARG" ;;
    s) SCHEMA_ONLY=true ;;
    t) TABLES="$OPTARG" ;;  # Override default tables with custom list
    h) 
      echo "Usage: $0 [-e env_file] [-o output_file] [-s] [-t tables]"
      echo "  -e: Environment file (default: .env.local)"
      echo "  -o: Output SQL file (default: ./tests/db_schema.sql)"
      echo "  -s: Schema only, no data (default: false)"
      echo "  -t: Tables to include, comma-separated (default: test-required tables)"
      echo ""
      echo "Default tables: $DEFAULT_TABLES"
      exit 0
      ;;
    \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
  esac
done

# Create the directory if it doesn't exist
mkdir -p "$DUMP_DIR"

# Extract database connection details from .env file
if [ ! -f "$ENV_FILE" ]; then
  echo "Error: Environment file $ENV_FILE not found!"
  exit 1
fi

# Parse database URL from env file to extract connection parameters
DB_URL=$(grep "^DATABASE_URL=" "$ENV_FILE" | cut -d= -f2- | tr -d '"')

# More robust parsing of connection string that handles URLs with or without passwords
if [[ "$DB_URL" =~ postgresql[^:]*://([^:@]+)(:([^@]+))?@([^:]+):([0-9]+)/([^?]+) ]]; then
  DB_USER="${BASH_REMATCH[1]}"
  DB_PASS="${BASH_REMATCH[3]}"  # This might be empty if no password in URL
  DB_HOST="${BASH_REMATCH[4]}"
  DB_PORT="${BASH_REMATCH[5]}"
  DB_NAME="${BASH_REMATCH[6]}"
else
  echo "Error: Could not parse DATABASE_URL from $ENV_FILE"
  echo "Expected format: postgresql[+driver]://<user>[:password]@<host>:<port>/<dbname>"
  exit 1
fi

echo "Dumping database schema and data from $DB_HOST:$DB_PORT/$DB_NAME to $DUMP_FILE..."
echo "Including tables: $TABLES"

# Set password temporarily in environment variable if it exists
if [ -n "$DB_PASS" ]; then
  export PGPASSWORD="$DB_PASS"
fi

# Convert comma-separated tables to array
IFS=',' read -ra TABLE_ARRAY <<< "$TABLES"

# Build table arguments for pg_dump
TABLE_ARGS=""
for table in "${TABLE_ARRAY[@]}"; do
  TABLE_ARGS="$TABLE_ARGS --table=$table"
done

# Dump schema only or schema and data based on the flag
if [ "$SCHEMA_ONLY" = true ]; then
  echo "Creating schema-only dump (no data)..."
  pg_dump --no-owner --no-acl --schema-only \
    --host="$DB_HOST" \
    --port="$DB_PORT" \
    --username="$DB_USER" \
    --dbname="$DB_NAME" \
    $TABLE_ARGS > "$DUMP_FILE"
else
  echo "Creating schema and sample data dump..."
  
  # First, create or truncate the output file and add schema
  pg_dump --no-owner --no-acl --schema-only \
    --host="$DB_HOST" \
    --port="$DB_PORT" \
    --username="$DB_USER" \
    --dbname="$DB_NAME" \
    $TABLE_ARGS > "$DUMP_FILE"
  
  # Add a transaction for all data inserts
  echo "" >> "$DUMP_FILE"
  echo "BEGIN;" >> "$DUMP_FILE"
  echo "" >> "$DUMP_FILE"
  
  # Process tables in the predefined order to respect foreign key constraints
  for ordered_table in "${ORDERED_TABLES[@]}"; do
    # Skip tables not in the requested list
    if [[ ! " ${TABLE_ARRAY[*]} " =~ " ${ordered_table} " ]]; then
      continue
    fi
    
    # Check if table exists
    if psql --host="$DB_HOST" --port="$DB_PORT" --username="$DB_USER" --dbname="$DB_NAME" -c "\dt $ordered_table" > /dev/null 2>&1; then
      echo "Adding sample data for table $ordered_table..."
      pg_dump --no-owner --no-acl --data-only \
        --host="$DB_HOST" \
        --port="$DB_PORT" \
        --username="$DB_USER" \
        --dbname="$DB_NAME" \
        --table="$ordered_table" | grep -v "^SET " | grep -v "^--" >> "$DUMP_FILE"
    else
      echo "Table $ordered_table does not exist, skipping..."
    fi
  done
  
  # End the transaction
  echo "" >> "$DUMP_FILE"
  echo "COMMIT;" >> "$DUMP_FILE"
  echo "" >> "$DUMP_FILE"
fi

# Clear password from environment if it was set
if [ -n "$DB_PASS" ]; then
  unset PGPASSWORD
fi

echo "Database schema and test data dumped successfully to $DUMP_FILE"
echo "This SQL file will be used to initialize the testing database."