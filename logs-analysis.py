#!/usr/bin/env python3
"""Report top articles, authors, and error days.

Report top 3 articles, author rankings, and days with errors above the
1% threshold.
"""
import psycopg2

db_name = 'missingnews'


def connect():
    """Connect to the PostgreSQL database, return a database connection."""
    return psycopg2.connect("dbname={}".format(db_name))


def printHeader():
    """Print a simple report header.

    Extract begin and end dates from the data.
    """
    db = connect()
    c = db.cursor()

    c.execute("SELECT min(time) FROM log;")
    start = c.fetchone()[0]

    c.execute("SELECT max(time) FROM log;")
    end = c.fetchone()[0]

    db.close()

    print("Site Activity Report")
    print("{} TO {}".format(
        start.date().isoformat(), end.date().isoformat()))


def defineTSNLView():
    """Define the title_slug_name_log (TSNL) view.

    Used for both the articles and authors reporting.
    """
    db = connect()
    c = db.cursor()

    # Create the views
    c.execute("""
        CREATE OR REPLACE VIEW log_article_200 AS
            SELECT replace(path, '/article/', '') as slug, time
            FROM log
            WHERE path != '/' and status = '200 OK';""")

    c.execute("""
        CREATE OR REPLACE VIEW title_slug_name AS
            SELECT title, slug, name
            FROM authors, articles
            WHERE authors.id = articles.author;""")

    c.execute("""
        CREATE OR REPLACE VIEW title_slug_name_log AS
            SELECT title, log_article_200.slug, name
            FROM log_article_200, title_slug_name
            WHERE log_article_200.slug = title_slug_name.slug;""")

    db.commit()
    db.close()


def printTopArticlesReport():
    """Output the top 3 articles to the console."""
    print("\n-1- TOP 3 ARTICLES\n")
    db = connect()
    c = db.cursor()
    c.execute("""
        SELECT title, count(*) AS views
        FROM title_slug_name_log
        GROUP BY title
        ORDER BY views DESC
        LIMIT 3;""")
    rows = c.fetchall()

    db.close
    print("{:^7}  {}".format("Views", "Title"))
    for title, views in rows:
        print("{:7}  {}".format(views, title))


def printTopAuthorsReport():
    """Output the authors rankings.

    Order according to page views from highest to lowest.
    """
    print("\n-2- TOP AUTHORS\n")
    db = connect()
    c = db.cursor()

    c.execute("""
        SELECT name, count(*) AS views
        FROM title_slug_name_log
        GROUP BY name
        ORDER BY views DESC;""")
    rows = c.fetchall()

    db.close()

    print("{:^7}  {}".format("Views", "Author"))
    for author, views in rows:
        print("{:7}  {}".format(views, author))


def printErrorReport():
    """Print days with greater than 1% error responses."""
    print("\n-3- DAYS WITH ERRORS OVER 1% THRESHOLD\n")

    db = connect()
    c = db.cursor()

    # Define new views
    c.execute("""
        CREATE OR REPLACE VIEW day_log AS
            SELECT time::date AS day, status FROM log;
        """)

    c.execute("""
        CREATE OR REPLACE VIEW log_200 AS
            SELECT day, count(*) AS okpd
                FROM day_log WHERE status = '200 OK'
                GROUP BY day;
        """)

    c.execute("""
        CREATE OR REPLACE VIEW log_404 AS
            SELECT day, count(*) AS nfpd
                FROM day_log
                WHERE status = '404 NOT FOUND'
                GROUP BY day;
        """)

    c.execute("""
        CREATE OR REPLACE VIEW log_200_404 AS
            SELECT log_200.day, okpd, nfpd
                FROM log_200 FULL OUTER JOIN log_404
                ON log_200.day = log_404.day
                ORDER BY day;
        """)

    c.execute("""
        CREATE OR REPLACE VIEW error_rate AS
            SELECT day, okpd, nfpd,
                round(
                    cast(
                        (cast(nfpd as real)*100)/
                            (cast(nfpd as real) + cast(okpd as real))
                        as numeric),
                    2) AS errp
                FROM log_200_404
                ORDER BY day;
        """)

    c.execute("""
        SELECT to_char(day, 'YYYY-MM-DD') as day, okpd, nfpd, errp
            FROM error_rate
            WHERE errp > 1;
        """)
    rows = c.fetchall()

    print("{:^10}  {}".format("Date", "Error Rate"))
    for day, okpd, nfpd, errp in rows:
        print("{:10}  {}".format(day, errp))

    db.commit()
    db.close()


if __name__ == '__main__':
    try:
        printHeader()
        defineTSNLView()
        printTopArticlesReport()
        printTopAuthorsReport()
        printErrorReport()
    except psycopg2.Error as e:
        print(e)
