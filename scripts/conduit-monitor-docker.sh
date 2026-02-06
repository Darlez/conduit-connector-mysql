#!/bin/bash
#
# Conduit Pipeline Monitor and Shutdown Script for Docker
#
# This script starts Conduit with configurable CLI parameters, monitors a pipeline,
# and shuts down when complete. Designed to run inside a Docker container.
#
# All parameters are configured via environment variables.
#

set -euo pipefail

# Configuration via environment variables
PIPELINE_ID="${PIPELINE_ID:-mysql-snapshot}"
API_HTTP_ADDRESS="${API_HTTP_ADDRESS:-:18080}"
PIPELINES_PATH="${PIPELINES_PATH:-/etc/conduit/pipelines/}"
LOG_FILE="${LOG_FILE:-/etc/conduit/conduit.log}"
LOG_LEVEL="${LOG_LEVEL:-info}"

CONDUIT_PID=""
SHUTDOWN_TIMEOUT=30
POLL_INTERVAL=2

# Extract port from API_HTTP_ADDRESS for internal API calls
# Handle formats like ":18080", "0.0.0.0:18080", "localhost:18080"
API_PORT=$(echo "$API_HTTP_ADDRESS" | sed 's/.*://')
API_URL="http://localhost:${API_PORT}/v1"

# Colors for output (disabled in non-TTY environments like Docker)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    NC='\033[0m'
else
    RED=''
    GREEN=''
    YELLOW=''
    NC=''
fi

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Check if required tools are available
check_dependencies() {
    if ! command -v curl &> /dev/null; then
        log_error "curl is required but not installed."
        exit 1
    fi
    
    if ! command -v jq &> /dev/null; then
        log_error "jq is required but not installed."
        exit 1
    fi
}

# Create log directory if needed
setup_logging() {
    local log_dir
    log_dir=$(dirname "$LOG_FILE")
    if [ ! -d "$log_dir" ]; then
        log_info "Creating log directory: $log_dir"
        mkdir -p "$log_dir"
    fi
    
    # Clear or create log file
    > "$LOG_FILE"
    log_info "Logging to: $LOG_FILE"
}

# Start Conduit with specified parameters
start_conduit() {
    log_info "Starting Conduit with configuration:"
    log_info "  --api.http.address $API_HTTP_ADDRESS"
    log_info "  --pipelines.path $PIPELINES_PATH"
    log_info "  --log.level $LOG_LEVEL"
    log_info "  Internal API URL: $API_URL"
    
    if ! command -v conduit &> /dev/null; then
        log_error "Conduit binary not found in PATH"
        exit 1
    fi
    
    # Start Conduit with CLI parameters, redirecting stdout and stderr to log file
    conduit run \
        --api.http.address "$API_HTTP_ADDRESS" \
        --pipelines.path "$PIPELINES_PATH" \
        --log.level "$LOG_LEVEL" \
        > "$LOG_FILE" 2>&1 &
    
    CONDUIT_PID=$!
    
    log_info "Conduit started with PID: $CONDUIT_PID"
    
    # Wait for Conduit to be ready
    log_info "Waiting for Conduit API to be ready at $API_URL..."
    local retries=0
    local max_retries=60
    
    while ! curl -s "$API_URL/pipelines" > /dev/null 2>&1; do
        if [ $retries -ge $max_retries ]; then
            log_error "Conduit failed to start after $max_retries attempts"
            log_error "Last 50 lines of log:"
            tail -n 50 "$LOG_FILE" || true
            kill -TERM "$CONDUIT_PID" 2>/dev/null || true
            exit 1
        fi
        
        if ! kill -0 "$CONDUIT_PID" 2>/dev/null; then
            log_error "Conduit process died unexpectedly during startup"
            log_error "Last 50 lines of log:"
            tail -n 50 "$LOG_FILE" || true
            exit 1
        fi
        
        sleep 1
        ((retries++))
    done
    
    log_info "Conduit API is ready"
}

# Get pipeline status
get_pipeline_status() {
    local response
    local http_code
    
    # Capture both response body and HTTP code
    response=$(curl -s -w "\n%{http_code}" "$API_URL/pipelines/$PIPELINE_ID" 2>/dev/null || echo -e "\n000")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | sed '$d')
    
    if [ "$http_code" == "000" ]; then
        echo "CONNECTION_ERROR"
        return
    fi
    
    if [ "$http_code" != "200" ]; then
        echo "API_ERROR:$http_code"
        return
    fi
    
    # Extract status from response
    echo "$body" | jq -r '.pipeline.status' 2>/dev/null || echo "PARSE_ERROR"
}

# Check if pipeline exists
wait_for_pipeline() {
    log_info "Waiting for pipeline '$PIPELINE_ID' to be registered..."
    local retries=0
    local max_retries=60
    
    while true; do
        local status
        status=$(get_pipeline_status)
        
        if [ "$status" != "CONNECTION_ERROR" ] && [ "$status" != "API_ERROR:404" ]; then
            log_info "Pipeline '$PIPELINE_ID' is registered"
            return 0
        fi
        
        if [ $retries -ge $max_retries ]; then
            log_error "Pipeline '$PIPELINE_ID' not found after $max_retries attempts"
            return 1
        fi
        
        if ! kill -0 "$CONDUIT_PID" 2>/dev/null; then
            log_error "Conduit process died while waiting for pipeline"
            return 1
        fi
        
        sleep 1
        ((retries++))
    done
}

# Monitor pipeline until it stops
monitor_pipeline() {
    log_info "Monitoring pipeline '$PIPELINE_ID'..."
    
    local last_status=""
    
    while true; do
        local status
        status=$(get_pipeline_status)
        
        # Log status changes
        if [ "$status" != "$last_status" ]; then
            log_info "Pipeline status changed to: $status"
            last_status=$status
        fi
        
        # Check if pipeline has stopped
        case "$status" in
            "STATUS_STOPPED"|"STATUS_USER_STOPPED"|"STATUS_SYSTEM_STOPPED")
                log_info "Pipeline stopped gracefully"
                return 0
                ;;
            "STATUS_DEGRADED")
                log_warn "Pipeline stopped with errors (degraded)"
                return 0
                ;;
            "CONNECTION_ERROR"|"API_ERROR"*)
                log_error "Failed to get pipeline status: $status"
                # Don't exit immediately, give it a few more tries
                ;;
            "STATUS_RUNNING"|"STATUS_STARTING"|"STATUS_RECOVERING")
                # Pipeline is still running, continue monitoring
                ;;
            *)
                log_warn "Unknown status: $status"
                ;;
        esac
        
        # Check if Conduit is still running
        if ! kill -0 "$CONDUIT_PID" 2>/dev/null; then
            log_error "Conduit process died unexpectedly"
            return 1
        fi
        
        sleep $POLL_INTERVAL
    done
}

# Gracefully shutdown Conduit
shutdown_conduit() {
    log_info "Initiating graceful shutdown of Conduit (PID: $CONDUIT_PID)..."
    
    # Send SIGTERM for graceful shutdown
    if kill -TERM "$CONDUIT_PID" 2>/dev/null; then
        log_info "Sent SIGTERM to Conduit, waiting up to ${SHUTDOWN_TIMEOUT}s for shutdown..."
        
        local waited=0
        while kill -0 "$CONDUIT_PID" 2>/dev/null; do
            if [ $waited -ge $SHUTDOWN_TIMEOUT ]; then
                log_warn "Graceful shutdown timeout reached, forcing kill..."
                kill -KILL "$CONDUIT_PID" 2>/dev/null || true
                break
            fi
            sleep 1
            ((waited++))
        done
        
        log_info "Conduit has been shut down"
    else
        log_warn "Conduit process already terminated"
    fi
}

# Output logs to stdout for Docker to capture
output_logs() {
    log_info "============================================"
    log_info "Conduit Log Output:"
    log_info "============================================"
    
    if [ -f "$LOG_FILE" ]; then
        cat "$LOG_FILE"
    else
        log_warn "Log file not found: $LOG_FILE"
    fi
    
    log_info "============================================"
}

# Check for errors in logs
check_logs_for_errors() {
    log_info "Checking logs for error messages..."
    
    if [ ! -f "$LOG_FILE" ]; then
        log_warn "Log file not found, cannot check for errors"
        return 0
    fi
    
    # Count error messages (pattern: " ERR ")
    # Use printf to ensure clean integer output
    local error_count
    error_count=$(grep -c " ERR " "$LOG_FILE" 2>/dev/null | head -1 || echo "0")
    error_count=$(printf '%s' "$error_count" | tr -d '\n\r')
    
    # Validate error_count is a number
    if ! [[ "$error_count" =~ ^[0-9]+$ ]]; then
        log_warn "Could not parse error count from logs, assuming no errors"
        return 0
    fi
    
    if [ "$error_count" -gt 0 ]; then
        log_error "Found $error_count error message(s) in logs"
        log_error "Error lines:"
        grep " ERR " "$LOG_FILE" || true
        return 1
    else
        log_info "No error messages found in logs"
        return 0
    fi
}

# Cleanup function
cleanup() {
    local exit_code=$?
    
    # Output logs regardless of exit status
    output_logs
    
    # Check for errors in logs
    if ! check_logs_for_errors; then
        exit_code=1
    fi
    
    if [ -n "$CONDUIT_PID" ] && kill -0 "$CONDUIT_PID" 2>/dev/null; then
        log_warn "Script interrupted or failed, shutting down Conduit..."
        kill -TERM "$CONDUIT_PID" 2>/dev/null || true
        sleep 2
        kill -KILL "$CONDUIT_PID" 2>/dev/null || true
    fi
    
    exit $exit_code
}

# Main execution
main() {
    # Set up trap for cleanup on script interruption
    trap cleanup EXIT INT TERM
    
    log_info "Starting Conduit Pipeline Monitor (Docker Version)"
    log_info "Configuration:"
    log_info "  PIPELINE_ID: $PIPELINE_ID"
    log_info "  API_HTTP_ADDRESS: $API_HTTP_ADDRESS"
    log_info "  PIPELINES_PATH: $PIPELINES_PATH"
    log_info "  LOG_FILE: $LOG_FILE"
    log_info "  LOG_LEVEL: $LOG_LEVEL"
    log_info "  API_URL: $API_URL"
    
    # Check dependencies
    check_dependencies
    
    # Setup logging
    setup_logging
    
    # Start Conduit
    start_conduit
    
    # Wait for pipeline to be registered
    if ! wait_for_pipeline; then
        exit 1
    fi
    
    # Monitor pipeline
    if ! monitor_pipeline; then
        log_error "Monitoring failed"
        exit 1
    fi
    
    # Shutdown Conduit
    shutdown_conduit
    
    log_info "Pipeline completed successfully"
    # Logs will be output by cleanup function
}

# Show usage
show_usage() {
    cat << EOF
Conduit Pipeline Monitor and Shutdown Script for Docker

This script starts Conduit with configurable CLI parameters, monitors a pipeline,
and shuts down when complete. All parameters are configured via environment variables.

Environment Variables:
  PIPELINE_ID         ID of the pipeline to monitor (default: mysql-snapshot)
  API_HTTP_ADDRESS    API bind address (default: :18080)
  PIPELINES_PATH      Path to pipeline configuration files (default: /etc/conduit/pipelines/)
  LOG_FILE            Path to log file (default: /etc/conduit/conduit.log)
  LOG_LEVEL           Log level: debug, info, warn, error (default: info)

Docker Compose Example:
  services:
    conduit-snapshot:
      image: conduit:latest
      environment:
        - PIPELINE_ID=mysql-snapshot
        - API_HTTP_ADDRESS=:18080
        - PIPELINES_PATH=/etc/conduit/pipelines/
        - LOG_FILE=/var/log/conduit.log
        - LOG_LEVEL=info
      volumes:
        - ./pipelines:/etc/conduit/pipelines/
        - ./logs:/var/log/
      command: /app/conduit-monitor-docker.sh

Docker CLI Example:
  docker run -d \\
    -e PIPELINE_ID=mysql-snapshot \\
    -e API_HTTP_ADDRESS=:18080 \\
    -e LOG_LEVEL=debug \\
    -v ./pipelines:/etc/conduit/pipelines/ \\
    -v ./logs:/var/log/ \\
    conduit-image \\
    /app/conduit-monitor-docker.sh

Exit codes:
  0 - Pipeline completed successfully, no errors in logs
  1 - Error occurred or error messages found in logs

EOF
}

# Handle help flag
if [ "${1:-}" == "-h" ] || [ "${1:-}" == "--help" ]; then
    show_usage
    exit 0
fi

# Run main
main
