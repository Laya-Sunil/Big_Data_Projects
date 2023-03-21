-- get all the records with database in tags 
SELECT * FROM c WHERE ARRAY_CONTAINS(c.tags, 'database')

-- get all the documents with author having exactly 2 courses

SELECT * FROM c WHERE c.author.profile.courses = 2

-- get all the documents with tags exactly matching 'language','freeware','programming'
SELECT * FROM c WHERE c.tags = ['language','freeware','programming']

-- get all the records with programming in tags 
SELECT * FROM c WHERE ARRAY_CONTAINS(c.tags, 'programming')

-- get all the documents with telugu in languages
SELECT * FROM c WHERE ARRAY_CONTAINS(c.languages, 'telugu')

-- get count of total number of documents
SELECT VALUE COUNT(1) FROM c

-- get first document
SELECT top 1 * FROM c

-- get documents with no of reviews>3 or tags contains programming
SELECT * FROM c WHERE c.no_of_reviews>3 and ARRAY_CONTAINS(c.tags, 'programming')

-- get documents with no of reviews<3 or downloadable is true or author profile contains 2 books
SELECT * FROM c WHERE c.no_of_reviews<3 or c.author.profile.books=2 or c.downloadable=true

-- get documents with no of reviews is not 3
SELECT * FROM c WHERE c.no_of_reviews<>3

-- get all the records with database, programming in tags 
SELECT * FROM c WHERE ARRAY_CONTAINS(c.tags, 'database') AND ARRAY_CONTAINS(c.tags, 'programming')
