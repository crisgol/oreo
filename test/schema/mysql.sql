SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS battles CASCADE;
DROP TABLE IF EXISTS samples CASCADE;
DROP TABLE IF EXISTS books CASCADE;
DROP TABLE IF EXISTS authors CASCADE;
DROP TABLE IF EXISTS Countries CASCADE;
DROP TABLE IF EXISTS ratings CASCADE;


CREATE TABLE Countries (
  Code VARCHAR(2),
  name TEXT,
  CONSTRAINT countries_pkey PRIMARY KEY(Code)
);

CREATE TABLE authors (
  id INTEGER AUTO_INCREMENT,
  name TEXT,
  books TEXT,
  Country VARCHAR(2),
  birthDate DATE,
  insertedAt TIMESTAMP DEFAULT now() NOT NULL,
  CONSTRAINT authors_pkey PRIMARY KEY(id),
  CONSTRAINT Country FOREIGN KEY (Country) REFERENCES Countries(Code)
);

CREATE TABLE books (
  id INTEGER AUTO_INCREMENT,
  title TEXT,
  author_id INTEGER,
  something VARCHAR(1),
  CONSTRAINT books_pkey PRIMARY KEY(id),
  CONSTRAINT author FOREIGN KEY (author_id) REFERENCES authors(id)
);

CREATE TABLE ratings (
  author_id INTEGER NOT NULL,
  book_id INTEGER NOT NULL,
  stars INTEGER,
  CONSTRAINT author1 FOREIGN KEY (author_id) REFERENCES authors(id),
  CONSTRAINT book1 FOREIGN KEY (book_id) REFERENCES books(id),
  CONSTRAINT ratings_pkey PRIMARY KEY(author_id, book_id),
  CONSTRAINT ratings_stars_key UNIQUE(stars)
);

create table reviews (
  id INTEGER AUTO_INCREMENT,
  book_id integer,
  stars integer,
  body varchar,
  constraint review_pkey primary key(id),
  CONSTRAINT book1 FOREIGN KEY (book_id) REFERENCES books(id)
);


CREATE TABLE `reviews` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `book_id` int(11) DEFAULT NULL,
  `stars` int(11) DEFAULT NULL,
  `body` text,

  CONSTRAINT reviews_pkey PRIMARY KEY(id),
  KEY `book2` (`book_id`),
  CONSTRAINT `book2` FOREIGN KEY (`book_id`) REFERENCES `books` (`id`)
)

CREATE TABLE samples (
  id INTEGER AUTO_INCREMENT,
  author_id INTEGER,
  book_id INTEGER,
  description TEXT,
  CONSTRAINT samples_pkey PRIMARY KEY(id),
  CONSTRAINT book FOREIGN KEY (book_id) REFERENCES books(id),
  CONSTRAINT rating FOREIGN KEY (author_id, book_id) REFERENCES ratings(author_id, book_id)
);

CREATE TABLE battles (
  id INTEGER AUTO_INCREMENT,
  author1_id INTEGER,
  author2_id INTEGER,
  CONSTRAINT battles_pkey PRIMARY KEY(id),
  CONSTRAINT a1 FOREIGN KEY (author1_id) REFERENCES authors(id),
  CONSTRAINT a2 FOREIGN KEY (author2_id) REFERENCES authors(id)
);