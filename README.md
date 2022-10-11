# Table of contents
## Purpose of this database
This database enables queries on song plays which are being accessed by different users. Sparkify can use the database to further make better individual offers to customers and ensure that only certain rights of music titles are bought in specific locations (for instance buy only rights for playing song xy 1000 times in the US, while for Europe only 10 rights are required)
The database facilitates to make queries for songs which are especially 
    1. Who uses the database
    2. Which songs are played
    3. Which artists are played
    4. At what time songs are accessed

## Launch
1. Run create_tables.py initially
2. Execute etl.py

## Explanation of the files in the repository
sql_queries.py: creates tables and insert statements for the database
test.ipynb: basic testing for the created tables and inserted data.
etl.ipynb: executes insert statements for individual tables. Attention: not all files are selected to insert data.
etl.py: selects all data from files and inserts selected data into the database. Attention: All files are selected. 
For entering data from log_files to the time_table the improved version of "copy_from" is used. If "copy_from" should not be used, the files sql.queries.py and etl.py have to be adjusted accordingly. 


## State and justify your database schema design and ETL pipeline
The database is built as a star schema. 
users, songs, artists and time when songs are played can be accessed individually.
The songplays table contains IDs which are connected to each individual table (users, songs, artists, time)
