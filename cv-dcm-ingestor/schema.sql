Create database xraymetadata;
use xraymetadata;

Create table sku_data(
id INT AUTO_INCREMENT NOT NULL,
imageId TEXT NOT NULL,
label TEXT,
company TEXT,
image_intent TEXT,
annotation TEXT,
height INT DEFAULT 224,
width INT DEFAULT 224,
image_status INT DEFAULT 0,
created_date DATETIME,
updated_date DATETIME,
PRIMARY KEY (id)
);
