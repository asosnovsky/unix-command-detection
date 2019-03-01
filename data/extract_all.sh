#!/bin/bash

INPUT="./00_gzs"
OUTPUT="./01_txt"

j=0

for fpath in $INPUT/*.gz; do
	fname="$(basename fpath)";
	fbasename="${fname%.*}";
	outf="$fbasename.txt";
	
	gunzip $fname -c > $outf
	echo "$j -> $fbasename";
	j=$(($j+1))
done
