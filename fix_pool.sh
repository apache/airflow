#!/bin/bash

# Read the file
input_file="airflow-core/src/airflow/models/pool.py"
temp_file="pool_temp.py"

# Process line by line
while IFS= read -r line; do
    echo "$line"
    
    # Add 2 blank lines after logger declaration
    if [[ "$line" == *"logger = logging.getLogger(__name__)"* ]]; then
        echo ""
        echo ""
    fi
    
    # Add 1 blank line after return normalized
    if [[ "$line" == *"return normalized"* ]] && [[ "$line" != *"normalized,"* ]]; then
        echo ""
    fi
    
done < "$input_file" > "$temp_file"

# Replace original
mv "$temp_file" "$input_file"

echo "Formatting fixed!"
