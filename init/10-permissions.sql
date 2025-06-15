-- For prototype project - NOT to be used with real data
-- (for production would use granular permissions per hostname)

-- Create user first (required in MySQL 8.0+)
CREATE USER IF NOT EXISTS 'university_user_id'@'%' IDENTIFIED BY 'university_user_pwd';

-- Grant permissions
GRANT ALL PRIVILEGES ON uni_dwh_db.* TO 'university_user_id'@'%';
GRANT SELECT ON uni_dwh_db.* TO 'university_user_id'@'%';
FLUSH PRIVILEGES;

-- Note: as this occurs only once during MySQL container start-up, these
-- create/grants also occur in setup.sh in case of transient issues.
