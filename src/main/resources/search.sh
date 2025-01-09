#!/bin/bash

# Usage: ./search_logs.sh "YYYY-MM-DD HH:MM:SS" /path/to/logs "Exception|Error"

# Input parameters
START_TIME="$1"
LOG_DIR="$2"
SEARCH_KEYWORDS="$3"

# Check if all arguments are provided
if [[ -z "$START_TIME" || -z "$LOG_DIR" || -z "$SEARCH_KEYWORDS" ]]; then
  echo "Usage: $0 \"YYYY-MM-DD HH:MM:SS\" /path/to/logs \"Exception|Error\""
  exit 1
fi

# Convert START_TIME to epoch
START_EPOCH=$(date -d "$START_TIME" +%s 2>/dev/null)
if [[ $? -ne 0 ]]; then
  echo "Invalid start time format. Use \"YYYY-MM-DD HH:MM:SS\"."
  exit 1
fi

# Find files modified or created after START_TIME and process them
find "$LOG_DIR" -type f -newermt "$START_TIME" | while read -r FILE; do
  echo "Processing file: $FILE"
  
  # Determine if the file is a .gz compressed file
  if [[ "$FILE" == *.gz ]]; then
    # Decompress and search for matching lines
    zcat "$FILE" | grep -E "$SEARCH_KEYWORDS" && echo "Found matches in $FILE"
  else
    # Search directly in regular text files
    grep -E "$SEARCH_KEYWORDS" "$FILE" && echo "Found matches in $FILE"
  fi
done
