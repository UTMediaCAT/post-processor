#!/usr/bin/env bash
# Rename files to match expected post processor input.
# Note: All files in the directory must be of type csv.

EXT=".csv"
HEAD="Output_"
MIDDLE="_output_0"
name=$0
directory=$1

echo "[MediaCAT] running $name"

if [ "$directory" = "" -o ! -d "$directory" ]
then
    echo "[MediaCAT] $directory is not a directory"
    exit 1
fi

for entry in "$directory"/*
do
    if [ "${entry: -4}" != "$EXT" ]
    then
        echo "[MediaCAT] files in $directory are not all $EXT type"
        exit 1
    fi
done

i=0
for entry in "$directory"/*
do
    direct=$(dirname "$entry")
    mv "$entry" "${direct}/${HEAD}${i}${MIDDLE}${EXT}"
    i=`expr $i + 1`
done

echo "[MediaCAT] rename finished"

