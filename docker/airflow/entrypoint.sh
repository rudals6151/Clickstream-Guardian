#!/bin/bash
set -e

# Initialize Airflow database if needed
if [ "$1" = "webserver" ] || [ "$1" = "scheduler" ]; then
    echo "Waiting for database to be ready..."
    sleep 5
    
    echo "Initializing Airflow database..."
    airflow db init || airflow db upgrade
    
    # Create admin user for webserver
    if [ "$1" = "webserver" ]; then
        echo "Creating admin user..."
        airflow users create \
            --username admin \
            --firstname Admin \
            --lastname User \
            --role Admin \
            --email admin@example.com \
            --password admin 2>/dev/null || true
    fi
fi

# Execute the main command
exec airflow "$@"
