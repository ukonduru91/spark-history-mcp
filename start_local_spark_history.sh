#!/bin/bash
# Start a local Spark History Server in Docker for testing the MCP.
#
# Port of the upstream project's script; same flags, same container name and
# image, reading the sample event logs from examples/basic/events.
set -e

show_help() {
    cat << EOF
🚀 Starting Local Spark History Server for MCP Testing
=======================================================

USAGE:
    ./start_local_spark_history.sh [OPTIONS]

OPTIONS:
    -h, --help                Show this help message
    --dry-run                 Validate prerequisites without starting the server
    --interactive             Run the Docker container in interactive mode
    --spark-version=VERSION   Spark version (default: 3.5.5)
    --event-dir=PATH          Host path to the Spark event directory (default: examples/basic)
    --container-name=NAME     Docker container name (default: spark-history-server)
    --port=PORT               Port to expose (default: 18080)

ENDPOINTS:
    - Web UI:   http://localhost:18080
    - REST API: http://localhost:18080/api/v1/
EOF
}

DRY_RUN=false
INTERACTIVE=false
SPARK_VERSION="3.5.5"
EVENT_DIR="examples/basic"
CONTAINER_NAME="spark-history-server"
PORT="18080"

for arg in "$@"; do
    case $arg in
        -h|--help) show_help; exit 0 ;;
        --dry-run) DRY_RUN=true ;;
        --interactive) INTERACTIVE=true ;;
        --spark-version=*) SPARK_VERSION="${arg#*=}" ;;
        --event-dir=*) EVENT_DIR="${arg#*=}" ;;
        --container-name=*) CONTAINER_NAME="${arg#*=}" ;;
        --port=*) PORT="${arg#*=}" ;;
        *) echo "Unknown option: $arg"; echo "Use --help for usage information."; exit 1 ;;
    esac
done

echo "🚀 Starting Local Spark History Server for MCP Testing"
echo "======================================================="

echo "🔍 Checking prerequisites..."
if ! docker info >/dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker Desktop first."
    exit 1
fi

if [ ! -d "$EVENT_DIR/events" ]; then
    echo "❌ Error: Test data directory '$EVENT_DIR/events' not found."
    exit 1
fi

if [ ! -f "$EVENT_DIR/history-server.conf" ]; then
    echo "❌ Error: Configuration file '$EVENT_DIR/history-server.conf' not found."
    exit 1
fi

echo "🛑 Stopping any existing Spark History Server containers..."
docker stop $CONTAINER_NAME 2>/dev/null && echo "   Stopped existing container" || echo "   No existing container found"
docker rm $CONTAINER_NAME 2>/dev/null || true

echo ""
echo "📁 Event directories found:"
ls -1 "$EVENT_DIR/events/" | sed 's/^/   /'

echo ""
echo "📋 Configuration:"
echo "   Log Directory: $(cat "$EVENT_DIR/history-server.conf")"
echo "   Port: $PORT"
echo "   Container: $CONTAINER_NAME"
echo "   Docker Image: apache/spark:$SPARK_VERSION"
echo ""
echo "📍 Web UI: http://localhost:$PORT"
echo "📍 API:    http://localhost:$PORT/api/v1/"
echo "⚠️  It may take 30-60 seconds for the server to fully start"
echo ""

if [ "$DRY_RUN" = true ]; then
    echo "✅ Dry run completed successfully! All prerequisites are met."
    exit 0
fi

DOCKER_FLAGS="--rm"
if [ "$INTERACTIVE" = true ]; then
    DOCKER_FLAGS="-it --rm"
fi

echo "🐳 Starting Docker container..."
docker run $DOCKER_FLAGS \
    --name $CONTAINER_NAME \
    --label "mcp-spark-test=true" \
    -v "$(pwd)/$EVENT_DIR:/mnt/data" \
    -p $PORT:18080 \
    docker.io/apache/spark:$SPARK_VERSION \
    /opt/java/openjdk/bin/java \
    -cp '/opt/spark/conf:/opt/spark/jars/*' \
    -Xmx1g \
    org.apache.spark.deploy.history.HistoryServer \
    --properties-file /mnt/data/history-server.conf

echo ""
echo "🛑 Spark History Server stopped."
