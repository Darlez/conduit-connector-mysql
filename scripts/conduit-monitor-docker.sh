#!/bin/bash
#
# Conduit Pipeline Monitor and Shutdown Script for Docker
#
# This script starts Conduit with configurable CLI parameters, monitors a pipeline,
# detects snapshot completion via log messages, and gracefully stops the pipeline via API.
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
SNAPSHOT_CHECK_INTERVAL=5
STARTUP_FAILED=0
SNAPSHOT_COMPLETED=0

# Extract port from API_HTTP_ADDRESS for internal API calls
API_PORT=$(echo "$API_HTTP_ADDRESS" | sed 's/.*://')
API_URL="http://localhost:${API_PORT}/v1"

# Colors for output (disabled in non-TTY environments like Docker)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m'
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    NC=''
fi

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
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
        return 1
    fi
    
    if ! command -v jq &> /dev/null; then
        log_error "jq is required but not installed."
        return 1
    fi
}

# Create log directory if needed
setup_logging() {
    local log_dir
    log_dir=$(dirname "$LOG_FILE")
    if [ ! -d "$log_dir" ]; then
        log_info "Creating log directory: $log_dir"
        mkdir -p "$log_dir" || {
            log_error "Failed to create log directory: $log_dir"
            return 1
        }
    fi
    
    # Clear or create log file
    > "$LOG_FILE" || {
        log_error "Failed to create log file: $LOG_FILE"
        return 1
    }
    log_info "Logging to: $LOG_FILE"
}

# Start Conduit with specified parameters
# Returns 0 on success, 1 on failure
start_conduit() {
    log_info "Starting Conduit with configuration:"
    log_info "  --api.http.address $API_HTTP_ADDRESS"
    log_info "  --pipelines.path $PIPELINES_PATH"
    log_info "  --log.level $LOG_LEVEL"
    log_info "  Internal API URL: $API_URL"
    
    if ! command -v conduit &> /dev/null; then
        log_error "Conduit binary not found in PATH"
        return 1
    fi
    
    # Check if pipelines path exists
    if [ ! -d "$PIPELINES_PATH" ]; then
        log_error "Pipelines path does not exist: $PIPELINES_PATH"
        return 1
    fi
    
    # List pipeline files for debugging
    log_info "Pipeline files in $PIPELINES_PATH:"
    ls -la "$PIPELINES_PATH" 2>/dev/null || log_warn "Could not list pipeline files"
    
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
    
    while true; do
        # Check if API is ready
        if curl -s "$API_URL/pipelines" > /dev/null 2>&1; then
            log_info "Conduit API is ready"
            return 0
        fi
        
        # Check if we've exceeded max retries
        if [ $retries -ge $max_retries ]; then
            log_error "Conduit failed to start after $max_retries attempts"
            log_error "Last 100 lines of log:"
            tail -n 100 "$LOG_FILE" 2>/dev/null || true
            return 1
        fi
        
        # Check if Conduit process is still running
        if ! kill -0 "$CONDUIT_PID" 2>/dev/null; then
            log_error "Conduit process died unexpectedly during startup"
            log_error "Last 100 lines of log:"
            tail -n 100 "$LOG_FILE" 2>/dev/null || true
            return 1
        fi
        
        sleep 1
        ((retries++))
        
        # Progress indicator every 10 seconds
        if [ $((retries % 10)) -eq 0 ]; then
            log_info "Still waiting for API... ($retries seconds)"
        fi
    done
}

# Get pipeline status
# Returns: STATUS_* string or error indicator
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
    
    # Extract status from response (format: state.status)
    echo "$body" | jq -r '.state.status' 2>/dev/null || echo "PARSE_ERROR"
}

# Check if pipeline exists
# Returns 0 on success, 1 on failure
wait_for_pipeline() {
    log_info "Waiting for pipeline '$PIPELINE_ID' to be registered..."
    local retries=0
    local max_retries=60
    
    while true; do
        local status
        status=$(get_pipeline_status)
        
        if [ "$status" != "CONNECTION_ERROR" ] && [ "$status" != "API_ERROR:404" ] && [ "$status" != "PARSE_ERROR" ]; then
            log_info "Pipeline '$PIPELINE_ID' is registered (status: $status)"
            return 0
        fi
        
        if [ $retries -ge $max_retries ]; then
            log_error "Pipeline '$PIPELINE_ID' not found after $max_retries attempts"
            log_error "Available pipelines:"
            curl -s "$API_URL/pipelines" 2>/dev/null | jq -r '.pipelines[].id' 2>/dev/null || echo "Could not list pipelines"
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

# Check if snapshot has completed by looking for the log message
# Returns 0 if completed, 1 if not
is_snapshot_complete() {
    if [ ! -f "$LOG_FILE" ]; then
        return 1
    fi
    
    # Look for the snapshot completion message
    if grep -q "snapshot completed in initial_only mode" "$LOG_FILE" 2>/dev/null; then
        return 0
    fi
    
    return 1
}

# Stop the pipeline via API
# Returns 0 on success, 1 on failure
stop_pipeline() {
    log_info "Stopping pipeline '$PIPELINE_ID' via API..."
    
    local response
    local http_code
    
    # Call the stop API with force=true to unblock blocked sources
    response=$(curl -s -w "\n%{http_code}" -X POST \
        "$API_URL/pipelines/$PIPELINE_ID/stop" \
        -H "Content-Type: application/json" \
        -d '{"force": true}' 2>/dev/null || echo -e "\n000")
    
    http_code=$(echo "$response" | tail -n1)
    
    if [ "$http_code" == "200" ] || [ "$http_code" == "202" ]; then
        log_info "Pipeline stop request accepted (HTTP $http_code)"
        return 0
    else
        log_error "Failed to stop pipeline (HTTP $http_code)"
        return 1
    fi
}

# Wait for pipeline to reach stopped state
# Returns 0 when stopped, 1 on error/timeout
wait_for_stopped() {
    log_info "Waiting for pipeline to stop..."
    local retries=0
    local max_retries=30
    
    while true; do
        local status
        status=$(get_pipeline_status)
        
        case "$status" in
            "STATUS_STOPPED"|"STATUS_USER_STOPPED"|"STATUS_SYSTEM_STOPPED")
                log_info "Pipeline is stopped"
                return 0
                ;;
            "STATUS_DEGRADED")
                log_warn "Pipeline is degraded (may have errors)"
                return 0
                ;;
            "CONNECTION_ERROR"|"API_ERROR"*)
                log_error "API error while waiting for stop: $status"
                return 1
                ;;
            "STATUS_RUNNING")
                # Still running, keep waiting
                ;;
            *)
                log_debug "Current status: $status"
                ;;
        esac
        
        if [ $retries -ge $max_retries ]; then
            log_warn "Timeout waiting for pipeline to stop, forcing..."
            return 1
        fi
        
        if ! kill -0 "$CONDUIT_PID" 2>/dev/null; then
            log_error "Conduit process died while waiting"
            return 1
        fi
        
        sleep 2
        ((retries++))
    done
}

# Monitor pipeline and detect snapshot completion
# Returns 0 on success, 1 on failure
monitor_and_stop() {
    log_info "Monitoring pipeline '$PIPELINE_ID' for snapshot completion..."
    
    local last_status=""
    local check_count=0
    
    while true; do
        local status
        status=$(get_pipeline_status)
        
        # Log status changes
        if [ "$status" != "$last_status" ]; then
            log_info "Pipeline status: $status"
            last_status=$status
        fi
        
        # Check if snapshot completed
        if [ $SNAPSHOT_COMPLETED -eq 0 ] && is_snapshot_complete; then
            log_info "Snapshot completion detected in logs!"
            SNAPSHOT_COMPLETED=1
            
            # Stop the pipeline via API
            if stop_pipeline; then
                # Wait for it to actually stop
                if wait_for_stopped; then
                    return 0
                else
                    return 1
                fi
            else
                return 1
            fi
        fi
        
        # Check for error states
        case "$status" in
            "STATUS_DEGRADED")
                log_warn "Pipeline is degraded, checking if snapshot completed..."
                if is_snapshot_complete; then
                    log_info "Snapshot was completed before degradation, treating as success"
                    return 0
                fi
                log_error "Pipeline degraded without completing snapshot"
                return 1
                ;;
            "STATUS_STOPPED"|"STATUS_USER_STOPPED"|"STATUS_SYSTEM_STOPPED")
                if is_snapshot_complete; then
                    log_info "Pipeline stopped after snapshot completion"
                    return 0
                else
                    log_error "Pipeline stopped before completing snapshot"
                    return 1
                fi
                ;;
            "CONNECTION_ERROR"|"API_ERROR"*)
                log_error "API error: $status"
                return 1
                ;;
        esac
        
        # Check if Conduit is still running
        if ! kill -0 "$CONDUIT_PID" 2>/dev/null; then
            log_error "Conduit process died unexpectedly"
            return 1
        fi
        
        # Log progress every minute
        ((check_count++))
        if [ $((check_count % 30)) -eq 0 ]; then
            log_info "Still monitoring... ($((check_count * POLL_INTERVAL)) seconds elapsed)"
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
# Returns 0 if no errors, 1 if errors found
check_logs_for_errors() {
    log_info "Checking logs for error messages..."
    
    if [ ! -f "$LOG_FILE" ]; then
        log_warn "Log file not found, cannot check for errors"
        return 0
    fi
    
    # Count error messages (pattern: " ERR ")
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

# Cleanup function - handles both normal shutdown and errors
cleanup() {
    local exit_code=$?
    
    # Determine if this is a startup failure
    if [ $STARTUP_FAILED -eq 1 ]; then
        log_error "Startup failed, outputting logs for debugging..."
        exit_code=1
    fi
    
    # Output logs regardless of exit status
    output_logs
    
    # Check for errors in logs
    if ! check_logs_for_errors; then
        exit_code=1
    fi
    
    # Shutdown Conduit if still running
    if [ -n "$CONDUIT_PID" ] && kill -0 "$CONDUIT_PID" 2>/dev/null; then
        log_warn "Shutting down Conduit..."
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
    if ! check_dependencies; then
        STARTUP_FAILED=1
        exit 1
    fi
    
    # Setup logging
    if ! setup_logging; then
        STARTUP_FAILED=1
        exit 1
    fi
    
    # Start Conduit
    if ! start_conduit; then
        STARTUP_FAILED=1
        exit 1
    fi
    
    # Wait for pipeline to be registered
    if ! wait_for_pipeline; then
        exit 1
    fi
    
    # Monitor pipeline and stop when snapshot completes
    if ! monitor_and_stop; then
        log_error "Monitoring/stopping failed"
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
detects snapshot completion via log messages, and gracefully stops the pipeline.
All parameters are configured via environment variables.

Environment Variables:
  PIPELINE_ID         ID of the pipeline to monitor (default: mysql-snapshot)
  API_HTTP_ADDRESS    API bind address (default: :18080)
  PIPELINES_PATH      Path to pipeline configuration files (default: /etc/conduit/pipelines/)
  LOG_FILE            Path to log file (default: /etc/conduit/conduit.log)
  LOG_LEVEL           Log level: debug, info, warn, error (default: info)

How it works:
1. Starts Conduit with the specified configuration
2. Monitors the pipeline status via API
3. Checks log file for "snapshot completed in initial_only mode" message
4. When detected, calls the API to stop the pipeline gracefully
5. Waits for pipeline to reach stopped state
6. Shuts down Conduit and exits

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
