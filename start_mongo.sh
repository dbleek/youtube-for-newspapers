#!/bin/bash

export MONGODB_PORT="27040"

mkdir -p /scratch/$USER/personal/db
mkdir -p /scratch/$USER/personal/ip
touch /scratch/$USER/personal/ip/ip_address.txt

# start the database container
singularity instance start --bind /scratch/$USER/personal/db:/data/db /scratch/work/public/singularity/mongo-7.0.4.sif mongo-db-$USER

# execute a command within container
#  print IP address to file
singularity exec instance://mongo-db-$USER echo $(hostname -i):$MONGODB_PORT > /scratch/$USER/personal/ip/ip_address.txt
# start mongodb
singularity exec instance://mongo-db-$USER /usr/bin/mongod --bind_ip_all --port $MONGODB_PORT &