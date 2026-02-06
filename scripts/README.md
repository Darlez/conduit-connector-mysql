# Conduit Pipeline Monitor Scripts

This directory contains scripts for monitoring Conduit pipelines and automatically shutting down the service when pipelines complete.

## Scripts

### 1. `conduit-monitor-docker.sh` (Docker Version - Recommended)

Production-ready script designed specifically for Docker containers. Uses environment variables for configuration and outputs logs to stdout for Docker logging.

#### Features

- **Environment-based configuration**: All settings via env vars
- **Docker-optimized**: Outputs logs to stdout for Docker capture
- **Error detection**: Checks logs for " ERR " pattern and exits with error code
- **Auto-generated API URL**: Derives from `--api.http.address` parameter
- **Graceful shutdown**: SIGTERM with timeout, then SIGKILL
- **Log file capture**: Redirects Conduit output to file, then cats to stdout

#### Configuration

All configuration is done via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `PIPELINE_ID` | ID of the pipeline to monitor | `mysql-snapshot` |
| `API_HTTP_ADDRESS` | Conduit API bind address | `:18080` |
| `PIPELINES_PATH` | Path to pipeline YAML files | `/etc/conduit/pipelines/` |
| `LOG_FILE` | Path to log file | `/etc/conduit/conduit.log` |
| `LOG_LEVEL` | Log level (debug, info, warn, error) | `info` |

#### Docker Compose Example

```yaml
version: '3.8'
services:
  mysql-snapshot:
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
      - ./scripts/conduit-monitor-docker.sh:/app/monitor.sh:ro
    command: /app/monitor.sh
    # Container stops automatically when pipeline completes
```

#### Docker CLI Example

```bash
docker run -d \
  --name conduit-mysql-snapshot \
  -e PIPELINE_ID=mysql-snapshot \
  -e API_HTTP_ADDRESS=:18080 \
  -e LOG_LEVEL=debug \
  -v $(pwd)/pipelines:/etc/conduit/pipelines/ \
  -v $(pwd)/logs:/var/log/ \
  -v $(pwd)/scripts/conduit-monitor-docker.sh:/app/monitor.sh:ro \
  conduit:latest \
  /app/monitor.sh
```

#### Kubernetes Example

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: mysql-snapshot
spec:
  template:
    spec:
      containers:
        - name: conduit
          image: conduit:latest
          env:
            - name: PIPELINE_ID
              value: "mysql-snapshot"
            - name: API_HTTP_ADDRESS
              value: ":18080"
            - name: PIPELINES_PATH
              value: "/etc/conduit/pipelines/"
            - name: LOG_FILE
              value: "/var/log/conduit.log"
            - name: LOG_LEVEL
              value: "info"
          volumeMounts:
            - name: pipelines
              mountPath: /etc/conduit/pipelines/
            - name: logs
              mountPath: /var/log/
            - name: monitor-script
              mountPath: /app/monitor.sh
              subPath: conduit-monitor-docker.sh
              readOnly: true
          command: ["/app/monitor.sh"]
      volumes:
        - name: pipelines
          configMap:
            name: conduit-pipelines
        - name: logs
          emptyDir: {}
        - name: monitor-script
          configMap:
            name: monitor-script
            defaultMode: 0755
      restartPolicy: Never
```

#### Pipeline Configuration Example

```yaml
# pipelines/mysql-snapshot.yaml
version: 2.2
pipelines:
  - id: mysql-snapshot
    status: running
    connectors:
      - id: mysql-source
        plugin: "builtin:mysql"
        settings:
          dsn: "user:password@tcp(mysql:3306)/production"
          tables: "users,orders,products"
          snapshot.enabled: "true"
          snapshot.mode: "initial_only"  # Stops after snapshot
      
      - id: file-destination
        plugin: "builtin:file"
        settings:
          path: "/backup/mysql-snapshot.jsonl"
```

#### How It Works

1. **Configuration**: Reads all settings from environment variables
2. **API URL**: Automatically generates `http://localhost:${API_PORT}/v1` from `API_HTTP_ADDRESS`
3. **Startup**: Runs `conduit run` with specified CLI parameters, redirecting output to log file
4. **Monitoring**: Polls `/v1/pipelines/{id}` every 2 seconds
5. **Completion detection**: Recognizes `STATUS_STOPPED`, `STATUS_DEGRADED`, etc.
6. **Shutdown**: Sends SIGTERM, waits 30s, then SIGKILL if needed
7. **Log output**: Cats log file to stdout for Docker to capture
8. **Error check**: Searches for " ERR " pattern, exits with code 1 if found

#### Exit Codes

| Code | Description |
|------|-------------|
| `0` | Success - Pipeline completed, no errors in logs |
| `1` | Error - Either pipeline failed or errors found in logs |

#### Viewing Logs

```bash
# View container logs (includes Conduit output)
docker logs conduit-mysql-snapshot

# View log file inside container
docker exec conduit-mysql-snapshot cat /var/log/conduit.log

# Follow logs in real-time
docker logs -f conduit-mysql-snapshot
```

#### Troubleshooting

**Container exits immediately:**
```bash
# Check if pipeline YAML is mounted correctly
docker run --rm -v $(pwd)/pipelines:/etc/conduit/pipelines/ conduit:latest ls /etc/conduit/pipelines/

# Run with debug logging
docker run -e LOG_LEVEL=debug ...
```

**Pipeline not found:**
- Verify `PIPELINE_ID` matches the ID in your YAML file
- Check that pipeline YAML is in `PIPELINES_PATH` directory
- Ensure pipeline YAML has `status: running`

**Connection errors:**
- The script auto-generates API URL from `API_HTTP_ADDRESS`
- If using a custom port, ensure it matches in both env vars

**Script hangs:**
- Check Conduit logs: `docker logs <container>`
- Verify MySQL is accessible from the container
- Check if pipeline is actually processing data

### 2. `conduit-monitor.sh` (Legacy/Standalone Version)

Original script for non-Docker environments. Takes command-line arguments and manages Conduit as a background process.

See the script comments for usage details.

## Choosing the Right Script

| Use Case | Script |
|----------|--------|
| Docker/Docker Compose | `conduit-monitor-docker.sh` |
| Kubernetes Jobs | `conduit-monitor-docker.sh` |
| Local development with Docker | `conduit-monitor-docker.sh` |
| Bare metal/VM without containers | `conduit-monitor.sh` |
| Systemd service | `conduit-monitor.sh` |

## Requirements

Both scripts require:
- `curl` - for API requests
- `jq` - for JSON parsing
- Conduit binary in PATH (or mounted in container)

## Security Considerations

- The scripts use `set -euo pipefail` for safe bash execution
- Log files may contain sensitive data - secure them appropriately
- In Docker, consider using read-only mounts for scripts (`:ro` flag)
- Use secrets management for database credentials in pipeline configs

## Contributing

When modifying these scripts:
1. Test in both Docker and standalone environments
2. Maintain backwards compatibility
3. Update this documentation
4. Consider edge cases (empty tables, connection failures, etc.)
