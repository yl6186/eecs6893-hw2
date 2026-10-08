#!/bin/bash
echo "Running hello.sh on $(hostname) at $(date)"
for i in 1 2 3; do
  echo "step $i"
  sleep 1
done
