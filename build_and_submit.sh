#!/usr/bin/env bash

# Define Flink command and job parameters
FLINK_CMD="$HOME/flink-1.20.1/bin/flink"
FLINK_MAIN_CLASS="com.example.flink.DataStreamJob"
FLINK_JOB_MANAGER="localhost:8081"
FLINK_PARALLELISM=1
FLINK_JAR_NAME="flink-hello-world-1.0-SNAPSHOT.jar"
FLINK_JOB_ARGS="" # Arguments for the Flink job, e.g., "input.txt output.txt"

echo "--- Starting Maven Flink project compilation ---"
# Compile the Maven project
mvn package

# Check if Maven compilation was successful
if [ $? -ne 0 ]; then
    echo "Maven compilation failed, stopping job submission."
    exit 1
fi

echo "--- Maven compilation successful, starting Flink job submission ---"

# Submit the Flink job
${FLINK_CMD} run \
    -c "${FLINK_MAIN_CLASS}" \
    -p "${FLINK_PARALLELISM}" \
    -m "${FLINK_JOB_MANAGER}" \
    "target/${FLINK_JAR_NAME}" \
    ${FLINK_JOB_ARGS}

# Check if Flink job submission was successful
if [ $? -ne 0 ]; then
    echo "Flink job submission failed."
    exit 1
fi

echo "--- Flink job submitted successfully! ---"
