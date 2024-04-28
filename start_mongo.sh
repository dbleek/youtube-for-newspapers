#!/bin/bash

# To run personal db, use command `source start_mongo.sh personal`
# To run shared db, use command `source start_mongo.sh shared`

export MONGODB_PORT="27040"

mkdir -p /scratch/$USER/$1/db
mkdir -p /scratch/$USER/$1/ip
touch /scratch/$USER/$1/ip/ip_address.txt

# start the database container
singularity instance start --bind /scratch/$USER/$1/db:/data/db /scratch/work/public/singularity/mongo-7.0.4.sif mongo-db-$USER

# execute a command within container
#  print IP address to file
singularity exec instance://mongo-db-$USER echo $(hostname -i):$MONGODB_PORT > /scratch/$USER/$1/ip/ip_address.txt
# start mongodb
singularity exec instance://mongo-db-$USER /usr/bin/mongod --bind_ip_all --port $MONGODB_PORT &