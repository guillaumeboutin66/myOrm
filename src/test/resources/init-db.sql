CREATE TABLE users (
  id BIGINT IDENTITY PRIMARY KEY,
  first_name VARCHAR(30) NOT NULL,
  last_name VARCHAR(30),
  email VARCHAR(50),
  birthDate DATE
);

INSERT INTO users (first_name, last_name, email, birthDate) VALUES ('Linus', 'Torvald', 'linux.torvald@linux.org', DATE '1969-12-28');
INSERT INTO users (first_name, last_name, email, birthDate) VALUES ('Brian', 'Goetz', 'brian.goetz@oracle.com', DATE '1970-11-22');
INSERT INTO users (first_name, last_name, email, birthDate) VALUES ('Robert', 'Martin', 'uncle@bob.com', DATE '1962-04-17');