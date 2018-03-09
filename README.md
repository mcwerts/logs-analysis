# Logs Analysis
Analyze the sample database for an imagined news site. Determine three things.
1. What were the top three articles on the news site according to the logs?
2. Who were the top authors for the site?
3. What days show greater than 1% of error responses?

## Requirements
* PostgreSQL 9.5.1
* Python 3
* Dataset [newsdata.zip](https://d17h27t6h515a5.cloudfront.net/topher/2016/August/57b5f748_newsdata/newsdata.zip)

## Setup Instructions
1. Create the `news` database in PostgreSQL
    * From the command line, launch the psql consol by typing: `psql`
    * Check to see if a `news` database already exists by listing all databases with the command: `\l`
    * If a `news` database already exists, drop it with the command: `DROP DATABASE news;`
    * Create the `news` database with the command: `CREATE DATABASE news;`
    * Exit the console by typing: `\q`
2. Download the schema and data for the `news` database:
    * [Click here to download](https://d17h27t6h515a5.cloudfront.net/topher/2016/August/57b5f748_newsdata/newsdata.zip)
3. Unzip the downloaded file. `unzip newsdata.zip`
    * You should now have an sql script called `newsdata.sql`.
4. From the command line, navigate to the directory containing `newsdata.sql`
5. Import the schema and data in `newsdata.sql` to the `news` database by typing:
`psql -d news -f newsdata.sql`

## Usage
Analyze the database.

`$ python3 logs-analysis.py`