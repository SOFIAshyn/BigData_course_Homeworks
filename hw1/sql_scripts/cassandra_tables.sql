CREATE TABLE Review_by_Product (
    product_id int,
    star_rating int,
    review_id text,
    review_headline text,
    review_body text,
    review_date timestamp,
    helpful_votes int,
    total_votes int,
    vine int,
    verified_purchase boolean,
    PRIMARY KEY (product_id)
) WITH CLUSTERING ORDER BY (star_rating ASC, review_id);


CREATE TABLE Review_by_Customer (
    customer_id int,
    review_id text,
    review_headline text,
    review_body text,
    review_date timestamp,
    verified_purchase boolean,
    PRIMARY KEY (customer_id)
) WITH CLUSTERING ORDER BY (review_id);


CREATE TABLE N_Customers_by_Star_reviews_and period (
    customer_id int,
    review_id text,
    review_date timestamp,
    star_rating int,
    verified_purchase boolean,
    PRIMARY KEY (customer_id)
) WITH CLUSTERING ORDER BY (review_id);


CREATE TABLE N_Products_by_Period_and_Num_of_reviews (
    product_id int,
    review_id text,
    star_rating int,
    review_date timestamp,
    helpful_votes int,
    total_votes int,
    product_title text,
    product_category text,
    PRIMARY KEY (product_id)
) WITH CLUSTERING ORDER BY (review_date, review_id);
