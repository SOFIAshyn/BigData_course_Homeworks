CREATE TABLE Review_Product_Customer (
id INT NOT NULL AUTO_INCREMENT,
marketplace VARCHAR(10),
customer_id INT,
review_id VARCHAR(200),
product_id VARCHAR(200),
PRIMARY KEY (id)
);

CREATE TABLE Review (
id INT NOT NULL AUTO_INCREMENT,
review_id VARCHAR(200) NOT NULL,
review_headline VARCHAR(600),
review_body VARCHAR(10000),
review_date DATE,
helpful_votes INT,
total_votes INT,
verified_purchase BOOL,
PRIMARY KEY (id)
);

CREATE TABLE Product (
id INT NOT NULL AUTO_INCREMENT,
product_id VARCHAR(200) NOT NULL,
product_parent INT,
product_title VARCHAR(600),
product_category VARCHAR(200),
star_rating INT,
PRIMARY KEY (id)
);
