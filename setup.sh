#!/bin/bash
# Create ETL log files
mkdir -p _mounts/logs
touch _mounts/logs/etl_info.log _mounts/logs/etl_error.log
chmod 666 _mounts/logs/etl_info.log _mounts/logs/etl_error.log
chmod 777 _mounts/logs _mounts/data

# Give current user ownership of data and logs directories
sudo chown -R $(id -u):$(id -g) _mounts/data _mounts/logs
chmod -R 755 _mounts/data

# Export current user's UID and GID for Docker (for data/log dir permissions)
# export UID=$(id -u)
# export GID=$(id -g)

# Start the MySQL container
echo "Starting MySQL container..."
docker compose up -d # no profile - starts MySQL but not ETL

# Wait for MySQL container to start
echo "Waiting 60 seconds for MySQL to be ready..."
sleep 60

# Grant privileges to ETL db user (wildcard covers connections
# from localhost or another container). Note this also occurs
# during 10-permissions.sql but only once during DB start-up.
echo "Setting up database permissions..."
ROOT_PWD=$(grep MYSQL_ROOT_PASSWORD .env | cut -d= -f2)
DB_NAME=$(grep MYSQL_DATABASE .env | cut -d= -f2)
ETL_USER=$(grep MYSQL_USER .env | cut -d= -f2)
ETL_PWD=$(grep MYSQL_PASSWORD .env | cut -d= -f2)

docker compose exec -e MYSQL_PWD="${ROOT_PWD}" mysql mysql -u root -e "
CREATE USER IF NOT EXISTS '${ETL_USER}'@'%' IDENTIFIED BY '${ETL_PWD}';
GRANT ALL PRIVILEGES ON \`${DB_NAME}\`.* TO '${ETL_USER}'@'%';
GRANT SELECT ON \`${DB_NAME}\`.* TO '${ETL_USER}'@'%';
FLUSH PRIVILEGES;
"

# Create date dimension and load tables
echo "Creating date dimension and load tables..."
docker compose run --rm app python utils/create_dim_date.py
docker compose run --rm app python utils/create_hesa_22056_load_tables.py
docker compose run --rm app python utils/create_hesa_23056_load_tables.py
docker compose run --rm app python utils/create_hesa_static_load_tables.py

echo "Date dimension and load tables created."