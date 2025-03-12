-- Create database
CREATE DATABASE IF NOT EXISTS cfpb_complaints;

-- Use the database
USE cfpb_complaints;

-- Create table for processed complaints
CREATE TABLE IF NOT EXISTS processed_complaints (
    id INT AUTO_INCREMENT PRIMARY KEY,
    complaint_id VARCHAR(255) UNIQUE,
    narrative TEXT NOT NULL,
    embedding JSON,  -- Store embedding as JSON array
    cluster_id INT NOT NULL,
    resolution VARCHAR(255),
    timely_response BOOLEAN,
    consumer_disputed BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster queries
CREATE INDEX idx_cluster_id ON processed_complaints(cluster_id);
CREATE INDEX idx_created_at ON processed_complaints(created_at);

-- Create view for dashboard
CREATE OR REPLACE VIEW recent_complaints AS
SELECT 
    cluster_id,
    resolution,
    COUNT(*) AS complaint_count,
    SUM(CASE WHEN timely_response = 1 THEN 1 ELSE 0 END) AS timely_responses,
    SUM(CASE WHEN consumer_disputed = 1 THEN 1 ELSE 0 END) AS disputed_complaints
FROM processed_complaints
WHERE created_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)
GROUP BY cluster_id, resolution;

-- Create user for the application
CREATE USER IF NOT EXISTS 'cfpb_app'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON cfpb_complaints.* TO 'cfpb_app'@'localhost';
FLUSH PRIVILEGES; 