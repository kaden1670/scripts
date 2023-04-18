#!/bin/bash
#Kaden DiMarco
#Banfield Lab 04/17/23

#split one column into two, alternating lines.

#!/bin/bash

# Parse command line arguments
while getopts "i:o:" opt; do
  case $opt in
    i) input_file="$OPTARG";;
    o) output_file="$OPTARG";;
    \?) echo "Invalid option -$OPTARG" >&2;;
  esac
done

# Initialize variables for first and second columns
col1=""
col2=""

# Loop through each line of the input file
while read line; do
  if [[ -z $col1 ]]; then
    col1=$line
  else
    col2=$line
    # Write the two columns to the output file
    echo -e "${col1}\t${col2}" >> $output_file
    # Reset the variables for the next pair of columns
    col1=""
    col2=""
  fi
done < $input_file