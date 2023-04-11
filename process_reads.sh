## Kaden DiMarco 04/10/2023
#Banfield Lab

#calculate the sequencing depths from compressed read files


#!/bin/bash

function usage {
    echo "Usage: bash process_reads.sh -i <input_file> -r <read_direction> -o <output_file>"
    echo "Given a tsv file containing paths to forward reads in the first column and paths to reverse reads in the second column, calculate the sequencing depth."
    echo "-h          explain function and exit"
    echo "-i          path to tsv file"
    echo "-r          read direction you would like depth calculated for (forward or reverse)"
    echo "-o          output file"
}

function process_reads() {
    while getopts ":hi:r:o:" opt; do
        case $opt in
            h) usage; exit ;;
            i) input_file="$OPTARG" ;;
            r) read_direction="$OPTARG" ;;
            o) output_file="$OPTARG" ;;
            \?) echo "Invalid option -$OPTARG" >&2 ;;
        esac
    done

    # Check that required options are set
    if [ -z "$input_file" ] || [ -z "$output_file" ] || [ -z "$read_direction" ]; then
        usage
        exit 1
    fi

    # Check that read direction is either "forward" or "reverse"
    if [ "$read_direction" != "forward" ] && [ "$read_direction" != "reverse" ]; then
        echo "Invalid read direction: $read_direction"
        usage
        exit 1
    fi

    # Extract the appropriate column based on the read direction
    if [ "$read_direction" == "forward" ]; then
        column_index=1
    else
        column_index=2
    fi

    # Process each read file in the input file
    tail -n +2 "$input_file" | awk -v column_index="$column_index" -F'\t' '{print $column_index}' | while read read_path; do
        read_name=$(echo "$read_path" | cut -d '/' -f 8)
        code=$(echo "$read_name" | cut -d '.' -f 1)
        printf "$code\t"
        zcat "$read_path" | awk 'BEGIN {tBases=0; tReads=0} {if (NR%4 == 2) tBases += length($0); if (NR%4 == 0) tReads += 1} END {print "Bases:\t" tBases "\tReads:\t" tReads}' | awk '{print "\t" $2 "\t" $4}'
    done > "$output_file"
}


   


