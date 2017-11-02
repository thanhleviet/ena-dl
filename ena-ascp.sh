#! /bin/bash
# An Aspera Connect wrapper for ENA downloading.
#
# Usage: ena-ascp FASTQ_URL OUTPUT_DIRECTORY
$ASCP -QT -l 300m -P33001 -i $ASCP_KEY $1 $2
