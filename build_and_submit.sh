#!/usr/bin/env bash

# Check number of arguments
if [ "$#" -lt 1 ]; then
    echo "Usage: ./s.sh {v1/v2} [args...]"
    echo "  {v1/v2}: Specifies the Flink main class version."
    echo "  [args...]: Optional arguments to pass to the Flink job."
    exit 1
fi

# Define Flink command and job parameters
FLINK_CMD="$HOME/flink-1.20.1/bin/flink"
FLINK_PARALLELISM=1
FLINK_JOB_MANAGER="localhost:8081"
FLINK_JAR_NAME="flink-hello-world-1.0-SNAPSHOT.jar"

# Set FLINK_MAIN_CLASS based on the first argument
VERSION_ARG="$1"
shift # Remove the first argument, remaining arguments will be FLINK_JOB_ARGS

case "$VERSION_ARG" in
"v1")
    FLINK_MAIN_CLASS="com.example.flink.TPCHQuery3JobV1"
    ;;
"v2")
    FLINK_MAIN_CLASS="com.example.flink.TPCHQuery3JobV2"
    ;;
*)
    echo "Error: Invalid version specified. Please use 'v1' or 'v2'."
    echo "Usage: ./s.sh {v1/v2} [args...]"
    exit 1
    ;;
esac

# Use remaining arguments as FLINK_JOB_ARGS
FLINK_JOB_ARGS="$@"

echo "--- Starting Maven Flink project compilation ---"
# Compile the Maven project
mvn package

# Check if Maven compilation was successful
if [ $? -ne 0 ]; then
    echo "Maven compilation failed, stopping job submission."
    exit 1
fi

echo "--- Maven compilation successful, starting Flink job submission ---"
echo "FLINK_MAIN_CLASS: ${FLINK_MAIN_CLASS}"
echo "FLINK_JOB_ARGS: ${FLINK_JOB_ARGS}"

# Submit the Flink job
${FLINK_CMD} run \
    -c "${FLINK_MAIN_CLASS}" \
    -p "${FLINK_PARALLELISM}" \
    -m "${FLINK_JOB_MANAGER}" \
    "target/${FLINK_JAR_NAME}" \
    ${FLINK_JOB_ARGS} # Note: no quotes here to correctly pass multiple arguments

# Check if Flink job submission was successful
if [ $? -ne 0 ]; then
    echo "Flink job submission failed."
    exit 1
fi

echo "--- Flink job submitted successfully! ---"
