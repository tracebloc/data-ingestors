use xraymetadata;

CREATE TABLE inspections_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    serialNo VARCHAR(255) NOT NULL,
    Field1 VARCHAR(255),
    Field2 VARCHAR(255),
    Field3 VARCHAR(255),
    Field4 VARCHAR(255),
    Field5 VARCHAR(255),
    Field6 VARCHAR(255),
    Field7 VARCHAR(255),
    Field8 VARCHAR(255),
    Field9 VARCHAR(255),
    Field10 VARCHAR(255),
    timestamp_x DATETIME,
    timestamp_y DATETIME,
    label VARCHAR(255),
    company VARCHAR(255) NOT NULL,
    status TINYINT NOT NULL,
    created_date DATETIME NOT NULL,
    updated_date DATETIME NOT NULL
);
