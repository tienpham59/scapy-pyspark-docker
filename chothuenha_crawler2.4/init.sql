CREATE TABLE IF NOT EXISTS house
(
    house_id VARCHAR NOT NULL,
    address VARCHAR NOT NULL,
    num_bedrooms INT NOT NULL,
    num_toilets INT NOT NULL,
    room_area INT NOT NULL,
    city VARCHAR NOT NULL,
    district VARCHAR NOT NULL,
    ward VARCHAR NOT NULL,
    street VARCHAR NOT NULL,
    PRIMARY KEY (house_id)
);

CREATE TABLE IF NOT EXISTS post
(
    post_id VARCHAR NOT NULL,
    views INT NOT NULL,
    type_listing VARCHAR NOT NULL,
    rental_price INT NOT NULL,
    title VARCHAR NOT NULL,
    poster VARCHAR NOT NULL, 
    content TEXT NOT NULL,
    house_id VARCHAR NOT NULL,  -- Thêm cột house_id
    PRIMARY KEY (post_id),
    FOREIGN KEY (house_id) REFERENCES house(house_id)  -- Định nghĩa khóa ngoại
);

CREATE TABLE IF NOT EXISTS amenity
(
    amenity_id VARCHAR NOT NULL,
    amenity_name VARCHAR NOT NULL,
    amenity_category VARCHAR NOT NULL,
    PRIMARY KEY (amenity_id)
);

CREATE TABLE IF NOT EXISTS post_amenity
(
    post_id VARCHAR NOT NULL,
    amenity_id VARCHAR NOT NULL,
    amenity_name VARCHAR NOT NULL,
    PRIMARY KEY (post_id, amenity_id),
    FOREIGN KEY (post_id) REFERENCES Post(post_id),
    FOREIGN KEY (amenity_id) REFERENCES Amenity(amenity_id)
);
