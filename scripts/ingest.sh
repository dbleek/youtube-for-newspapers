#!/bin/bash

read -p "Enter publication title: " title

# Clear tmp dir
rm -r $SCRATCH/youtube-for-newspapers-cache/tmp
mkdir $SCRATCH/youtube-for-newspapers-cache/tmp

# Run main.py
cd $SCRATCH/youtube-for-newspapers
python src/main.py --test --cache --data_raw /scratch/work/public/proquest/proquest_hnp/$title
