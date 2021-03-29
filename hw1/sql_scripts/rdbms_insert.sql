CREATE TABLE Review_Product_Customer (
id INT NOT NULL AUTO_INCREMENT,
marketplace VARCHAR(10),
customer_id INT,
review_id VARCHAR(200),
product_id VARCHAR(200),
PRIMARY KEY (id)
);

CREATE TABLE Review (
id VARCHAR(200) NOT NULL AUTO_INCREMENT,
review_headline VARCHAR(600),
review_body VARCHAR(10000),
review_date DATE,
helpful_votes INT,
total_votes INT,
verified_purchase BOOL,
PRIMARY KEY (id)
);

CREATE TABLE Product (
id VARCHAR(200) NOT NULL AUTO_INCREMENT,
product_parent INT,
product_title VARCHAR(600),
product_category VARCHAR(200),
star_rating INT,
PRIMARY KEY (id)
);

INSERT INTO Review_Product_Customer (marketplace, customer_id, review_id, product_id) VALUES (US, 25933450, RJOVP071AVAJO, 0439873800);
INSERT INTO Review (review_headline, review_body, review_date, helpful_votes, total_votes, verified_purchase) VALUES (Five Stars, I love it and so does my students!, 2015-08-31, 0, 0, True);
INSERT INTO Product (product_parent, product_title, product_category, star_rating) VALUES (84656342, There Was an Old Lady Who Swallowed a Shell!, Books, 5);
INSERT INTO Review_Product_Customer (marketplace, customer_id, review_id, product_id) VALUES (US, 1801372, R1ORGBETCDW3AI, 1623953553);
INSERT INTO Review (review_headline, review_body, review_date, helpful_votes, total_votes, verified_purchase) VALUES (Please buy \"I Saw a Friend\"! Your children will be delighted!, My wife and I ordered 2 books and gave them as presents...one to a friend\"s daughter and the other to our grandson! Both children were so happy with the story, by author Katrina Streza, and they were overjoyed with the absolutely adorable artwork, by artist Michele Katz, throughout the book! We highly recommend &#34;I Saw a Friend&#34; to all your little ones!!!, 2015-08-31, 0, 0, True);
INSERT INTO Product (product_parent, product_title, product_category, star_rating) VALUES (729938122, I Saw a Friend, Books, 5);
INSERT INTO Review_Product_Customer (marketplace, customer_id, review_id, product_id) VALUES (US, 5782091, R7TNRFQAOUTX5, 142151981X);
INSERT INTO Review (review_headline, review_body, review_date, helpful_votes, total_votes, verified_purchase) VALUES (Shipped fast., Great book just like all the others in the series., 2015-08-31, 0, 0, True);
INSERT INTO Product (product_parent, product_title, product_category, star_rating) VALUES (678139048, Black Lagoon, Vol. 6, Books, 5);
INSERT INTO Review_Product_Customer (marketplace, customer_id, review_id, product_id) VALUES (US, 32715830, R2GANXKDIFZ6OI, 014241543X);
INSERT INTO Review (review_headline, review_body, review_date, helpful_votes, total_votes, verified_purchase) VALUES (Five Stars, So beautiful, 2015-08-31, 0, 0, False);
INSERT INTO Product (product_parent, product_title, product_category, star_rating) VALUES (712432151, If I Stay, Books, 5);
INSERT INTO Review_Product_Customer (marketplace, customer_id, review_id, product_id) VALUES (US, 14005703, R2NYB6C3R8LVN6, 1604600527);
INSERT INTO Review (review_headline, review_body, review_date, helpful_votes, total_votes, verified_purchase) VALUES (Five Stars, Enjoyed the author\"s story and his quilts are incredible.  I have plans to make three., 2015-08-31, 2, 2, True);
INSERT INTO Product (product_parent, product_title, product_category, star_rating) VALUES (800572372, Stars 'N Strips Forever, Books, 5);
INSERT INTO Review_Product_Customer (marketplace, customer_id, review_id, product_id) VALUES (US, 36205738, R13U5PBJI1H94K, 0399170863);
INSERT INTO Review (review_headline, review_body, review_date, helpful_votes, total_votes, verified_purchase) VALUES (PREDICTABLE ALMOST FROM PAGE 1, Two or three pages into the book I suspected how it would end.  And after another 10 or 20 pages, I was sure.  And I was right.  So much for suspense.  The best I could say about this book is that it is okay.  Just barely.  It\"s a long book -- unnecessarily long -- and when the author hasn\"t taken the trouble to craft her plot in a suspenseful way, there\"s not a lot there to make you want to turn the page.  I have mixed feelings about her main character, who was supposed to have been a vibrant, energetic, assertive woman, yet became weak and submissive to her manipulative husband.  It\"s hard for any reader to identiify with that.   Additionally, the romance in the book isn\"t all that interesting -- just two people who are attracted to each other.  No real getting-to-know-you development, no build-up of heat, no surprise, really.    This 500-page book is really 250 pages too long and it all feels stretched out, with long sequences of unnecessary dialogue, superfluous scenes to establish things that already have been established, and so on.    Can\"t help wondering what\"s going on with Nora Roberts these days.  She\"s written a number of really good books, but her recent books seem like &#34;Nora Roberts diluted&#34;.  So-so, at best.  Maybe with so many books behind her, she\"s just burning out.  As far as this one goes, I\"m glad I got it from the library.  If I had paid for it, I\"d have been steamed.  Maybe if she wrote fewer books each year -- but better ones -- I\"d be a happier reader., 2015-08-31, 1, 1, False);
INSERT INTO Product (product_parent, product_title, product_category, star_rating) VALUES (559876774, The Liar, Books, 2);
