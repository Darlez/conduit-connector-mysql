#!/bin/bash
#
# Conduit Pipeline Monitor and Shutdown Script
#
# This script monitors a Conduit pipeline and shuts down the Conduit service
# when the pipeline stops (either gracefully or with an error).
#
# Usage: ./conduit-monitor.sh [PIPELINE_ID] [CONDUIT_API_URL] [CONDUIT_BIN_PATH]
#

set -euo pipefail

# Configuration
PIPELINE_ID="${1:-mysql-snapshot}"
API_URL="${2:-http://localhost:8080/v1}"
CONDUIT_BIN="${3:-conduit}"
CONDUIT_PID=""
SHUTDOWN_TIMEOUT=30
POLL_INTERVAL=2

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

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

# Start Conduit in the background
start_conduit() {
    log_info "Starting Conduit..."
    
    if ! command -v "$CONDUIT_BIN" &> /dev/null; then
        log_error "Conduit binary not found at: $CONDUIT_BIN"
        log_error "Please provide the correct path as the third argument."
        exit 1
    fi
    
    # Start Conduit in background and capture PID
    "$CONDUIT_BIN" &
    CONDUIT_PID=$!
    
    log_info "Conduit started with PID: $CONDUIT_PID"
    
    # Wait for Conduit to be ready
    log_info "Waiting for Conduit API to be ready..."
    local retries=0
    local max_retries=30
    
    while ! curl -s "$API_URL/pipelines" > /dev/null 2>&1; do
        if [ $retries -ge $max_retries ]; then
            log_error "Conduit failed to start after $max_retries attempts"
            kill -TERM "$CONDUIT_PID" 2>/dev/null || true
            exit 1
        fi
        
        if ! kill -0 "$CONDUIT_PID" 2>/dev/null; then
            log_error "Conduit process died unexpectedly"
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

# Cleanup function
cleanup() {
    if [ -n "$CONDUIT_PID" ] && kill -0 "$CONDUIT_PID" 2>/dev/null; then
        log_warn "Script interrupted, shutting down Conduit..."
        kill -TERM "$CONDUIT_PID" 2>/dev/null || true
        sleep 2
        kill -KILL "$CONDUIT_PID" 2>/dev/null || true
    fi
}

# Main execution
main() {
    # Set up trap for cleanup on script interruption
    trap cleanup EXIT INT TERM
    
    log_info "Starting Conduit Pipeline Monitor"
    log_info "Pipeline ID: $PIPELINE_ID"
    log_info "API URL: $API_URL"
    log_info "Conduit Binary: $CONDUIT_BIN"
    
    # Check dependencies
    check_dependencies
    
    # Start Conduit
    start_conduit
    
    # Wait for pipeline to be registered
    if ! wait_for_pipeline; then
        shutdown_conduit
        exit 1
    fi
    
    # Monitor pipeline
    if ! monitor_pipeline; then
        log_error "Monitoring failed"
        shutdown_conduit
        exit 1
    fi
    
    # Shutdown Conduit
    shutdown_conduit
    
    log_info "Script completed successfully"
}

# Show usage
show_usage() {
    cat << EOF
Usage: $0 [PIPELINE_ID] [API_URL] [CONDUIT_BIN_PATH]

Monitor a Conduit pipeline and shut down Conduit when the pipeline stops.

Arguments:
  PIPELINE_ID       ID of the pipeline to monitor (default: mysql-snapshot)
  API_URL          Conduit API URL (default: http://localhost:8080/v1)
  CONDUIT_BIN_PATH Path to Conduit binary (default: conduit)

Examples:
  $0                                    # Use defaults
  $0 my-pipeline                        # Monitor specific pipeline
  $0 my-pipeline http://localhost:8080/v1 /usr/local/bin/conduit

Requirements:
  - curl
  - jq
  - Conduit binary in PATH or specified path

Exit codes:
  0 - Pipeline completed successfully, Conduit shut down
  1 - Error occurred

EOF
}

# Handle help flag
if [ "${1:-}" == "-h" ] || [ "${1:-}" == "--help" ]; then
    show_usage
    exit 0
fi

# Run main
main
