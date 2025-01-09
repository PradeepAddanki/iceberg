Share


You said:
shell scripts which can input as start time , location log files  and search for  exception or error is log file .gz files
ChatGPT said:
ChatGPT
Here's a shell script that takes the following inputs:

Start Time - The start time from which to search in the log files.
Location of Log Files - The directory where .gz log files are stored.
Search Keywords - Keywords like "Exception" or "Error" to find in the logs.
The script decompresses .gz files, searches for the given keywords after the specified start time, and displays the results.

bash
Copy code
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

# Convert START_TIME to epoch for comparison
START_EPOCH=$(date -d "$START_TIME" +%s 2>/dev/null)
if [[ $? -ne 0 ]]; then
  echo "Invalid start time format. Use \"YYYY-MM-DD HH:MM:SS\"."
  exit 1
fi

# Process each .gz file in the log directory
find "$LOG_DIR" -type f -name "*.gz" | while read -r LOG_FILE; do
  echo "Processing file: $LOG_FILE"
  
  # Decompress the file and search for matching lines
  zcat "$LOG_FILE" | awk -v start_epoch="$START_EPOCH" -v keywords="$SEARCH_KEYWORDS" '
  BEGIN {IGNORECASE=1}
  {
    # Extract timestamp from log entry (assuming it starts at the beginning of the line)
    if ($0 ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}/) {
      split($0, fields, " ");
      log_time = fields[1] " " fields[2];
      log_epoch = mktime(gensub(/[-:]/, " ", "g", log_time));
    }
    # Print lines that match the keywords and occur after START_TIME
    if (log_epoch >= start_epoch && $0 ~ keywords) {
      print $0;
    }
  }'
done
How to Use:
Save the script as search_logs.sh.
Make it executable:
bash
Copy code
chmod +x search_logs.sh
Run the script with appropriate arguments:
bash
Copy code
./search_logs.sh "2025-01-01 12:00:00" /path/to/logs "Exception|Error"
Notes:
Replace the timestamp parsing logic in the script with the specific log format if it differs.
The script assumes timestamps are at the beginning of the log entry.
If the logs use a different date format, adjust the awk section to parse the timestamps accordingly.
Add error handling for large files if needed.





You said:
shell scripts which can input as time, and location log files and search for exceptions or errors in log files and gz files in the file modified or created after the given time? 
ChatGPT said:
ChatGPT
Here's a shell script that takes the following inputs:

Start Time - The time after which files were created or modified.
Log Directory - The directory where the log files and .gz files are stored.
Search Keywords - Keywords like "Exception" or "Error" to search within the logs.
This script filters the files based on their modification/creation time, decompresses .gz files if necessary, and searches for matching lines containing the specified keywords.

Script:
bash
Copy code
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
How to Use:
Save the script as search_logs.sh.
Make it executable:
bash
Copy code
chmod +x search_logs.sh
Run the script with appropriate arguments:
bash
Copy code
./search_logs.sh "2025-01-01 12:00:00" /path/to/logs "Exception|Error"
Script Explanation:
find Command:

Finds all files in the given directory ($LOG_DIR) that were modified or created after the provided start time ($START_TIME) using -newermt.
Processing .gz Files:

If the file has a .gz extension, it decompresses it using zcat and searches for the keywords.
Searching in Plain Text Files:

For normal text log files, it directly searches using grep.
Regular Expressions:

The grep -E allows for extended regular expressions, so you can provide multiple keywords separated by | (e.g., Exception|Error).
Notes:
Ensure the date command supports the -d option (common in GNU/Linux systems).
This script is case-sensitive by default. Add -i to grep for case-insensitive search:
bash
Copy code
grep -Ei "$SEARCH_KEYWORDS"
If the log format has special structures (e.g., timestamps), modify accordingly.





You said:
shell scripts which can input as time, and location log files and search for exceptions or errors in log files and gz files in the file modified or created after the given time with line number and after the match 5 more lines? 
ChatGPT said:
ChatGPT
Here's a shell script that takes the following inputs:

Start Time - The time after which files were modified or created.
Log Directory - The directory where the log files and .gz files are stored.
Search Keywords - Keywords like "Exception" or "Error" to search within the logs.
The script identifies relevant files, searches for the keywords, includes the line numbers of the matches, and displays the next 5 lines after each match.

Shell Script:
bash
Copy code
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
    # Decompress and search for matching lines with context
    zcat "$FILE" | grep -E -n -A 5 "$SEARCH_KEYWORDS" && echo "Found matches in $FILE"
  else
    # Search directly in regular text files with context
    grep -E -n -A 5 "$SEARCH_KEYWORDS" "$FILE" && echo "Found matches in $FILE"
  fi
done
Explanation of the Script:
find Command:

Filters files in the directory ($LOG_DIR) modified or created after $START_TIME.
File Processing:

For .gz files, it uses zcat to decompress and pipe the output to grep.
For plain text files, it directly applies grep.
grep -n -A 5:

-n: Displays the line number of the matching lines.
-A 5: Displays 5 lines after each matching line.
Output:

Displays matching lines with their line numbers and up to 5 additional lines of context.
How to Use:
Save the script as search_logs.sh.
Make it executable:
bash
Copy code
chmod +x search_logs.sh
Run the script with appropriate arguments:
bash
Copy code
./search_logs.sh "2025-01-01 12:00:00" /path/to/logs "Exception|Error"
Notes:
Case-Insensitive Search: To make the search case-insensitive, add the -i flag to grep:
bash
Copy code
grep -Ei -n -A 5 "$SEARCH_KEYWORDS"
Logs Without Timestamps: If the log files don't have clear modification/creation times, consider an alternative approach for filtering.
Error Handling: The script assumes all .gz files can be decompressed. Handle decompression errors as needed.





You said:
With The file name attached on starting of each line of the result 
ChatGPT said:
ChatGPT
Here’s the updated script. It prepends the filename to each matching line in the results, along with the line number, and includes the 5 lines following each match.

Shell Script:
bash
Copy code
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
    # Decompress and search for matching lines with context
    zcat "$FILE" | grep -E -n -A 5 "$SEARCH_KEYWORDS" | sed "s|^|$FILE: |"
  else
    # Search directly in regular text files with context
    grep -E -n -A 5 "$SEARCH_KEYWORDS" "$FILE" | sed "s|^|$FILE: |"
  fi
done
