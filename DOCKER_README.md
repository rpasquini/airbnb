# PostgreSQL Docker Setup

This repository contains a Docker Compose configuration for running PostgreSQL 15.

## Prerequisites

- Docker
- Docker Compose

## Configuration

The PostgreSQL database is configured with the following settings:

- **Port**: 5435 (mapped to container port 5432)
- **Database Name**: airbnb
- **Username**: postgres
- **Password**: postgres
- **Container Name**: airbnb-db
- **Volume Name**: airbnb_postgres_data

## Getting Started

1. Start the database:
   ```bash
   docker-compose up -d
   ```

2. Stop the database:
   ```bash
   docker-compose down
   ```

3. View logs:
   ```bash
   docker-compose logs -f
   ```

## Connection Details

To connect to the database using a PostgreSQL client:

- **Host**: localhost
- **Port**: 5435
- **Database**: airbnb
- **Username**: postgres
- **Password**: postgres

Example connection string:
```
postgresql://postgres:postgres@localhost:5435/airbnb
```

## Data Persistence

The database data is persisted using a Docker volume named `airbnb_postgres_data`. This ensures that:
- Your data remains intact even if the container is stopped or removed
- The data is isolated to this specific project
- You can easily backup or migrate the data

To manage the volume:
```bash
# List volumes
docker volume ls

# Inspect volume details
docker volume inspect airbnb_postgres_data

# Backup volume (example)
docker run --rm -v airbnb_postgres_data:/source -v $(pwd):/backup alpine tar -czf /backup/postgres_backup.tar.gz -C /source .
```

## Security Note

The default credentials in this setup are for development purposes only. For production use, please:

1. Change the default password
2. Use environment variables for sensitive data
3. Implement proper security measures

## Troubleshooting

If you encounter any issues:

1. Check if the container is running:
   ```bash
   docker ps
   ```

2. Check container logs:
   ```bash
   docker-compose logs
   ```

3. Verify port availability:
   ```bash
   netstat -an | grep 5435
   ```

4. Check volume status:
   ```bash
   docker volume ls | grep airbnb_postgres_data
   ``` 