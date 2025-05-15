#!/bin/bash

# Infinite loop to rerun the Go program
while true; do
    echo "Starting Go program..."
    go run indexer.go

    # Exit message
    echo "Program exited. Restarting in 5 seconds..."
    sleep 5
done