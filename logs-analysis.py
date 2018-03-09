#!/usr/bin/env python3
"""Report top articles, authors, and error days.

Report top 3 articles, author rankings, and days with errors above the
1% threshold.
"""
import psycopg2


def connect():
    """Connect to the PostgreSQL database, return a database connection."""
    return psycopg2.connect("dbname=news")


def printHeader():
    """Print a simple report header.

    Extract begin and end dates from the data.
    """
    db = connect()
    c = db.cursor()

    c.execute("SELECT min(time) FROM log;")
    rows = c.fetchall()
    start = rows[0][0]

    c.execute("SELECT max(time) FROM log;")
    rows = c.fetchall()
    end = rows[0][0]

    db.commit()
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

    # The order of these view operations is IMPORTANT. The
    # list is in reverse dependency order.
    c.execute("DROP VIEW IF EXISTS title_slug_name_log;")
    c.execute("DROP VIEW IF EXISTS log_article_200;")
    c.execute("DROP VIEW IF EXISTS title_slug_name;")

    # Create the views
    c.execute("""
        CREATE VIEW log_article_200 AS
            SELECT replace(path, '/article/', '') as slug, time
            FROM log
            WHERE path != '/' and status = '200 OK';""")

    c.execute("""
        CREATE VIEW title_slug_name AS
            SELECT title, slug, name
            FROM authors, articles
            WHERE authors.id = articles.author;""")

    c.execute("""
        CREATE VIEW title_slug_name_log AS
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

    db.commit()
    db.close
    print("{:^7}  {}".format("Views", "Title"))
    for i, row in enumerate(rows):
        print("{:7}  {}".format(rows[i][1], rows[i][0]))


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

    db.commit()
    db.close()

    print("{:^7}  {}".format("Views", "Author"))
    for i, row in enumerate(rows):
        print("{:7}  {}".format(rows[i][1], rows[i][0]))


def printErrorReport():
    """Print days with greater than 1% error responses."""
    print("\n-3- DAYS WITH ERRORS OVER 1% THRESHOLD\n")

    db = connect()
    c = db.cursor()

    # Drop any pre-existing views
    c.execute("DROP VIEW IF EXISTS errors_over_1p;")
    c.execute("DROP VIEW IF EXISTS log_200_404;")
    c.execute("DROP VIEW IF EXISTS log_200;")
    c.execute("DROP VIEW IF EXISTS log_404;")
    c.execute("DROP VIEW IF EXISTS day_log;")

    # Define new views
    c.execute("""
        CREATE VIEW day_log AS
            SELECT to_char(time, 'YYYY-MM-DD') AS day, status FROM log;
        """)

    c.execute("""
        CREATE VIEW log_200 AS
            SELECT day, count(*) AS okpd
                FROM day_log WHERE status = '200 OK'
                GROUP BY day;
        """)

    c.execute("""
        CREATE VIEW log_404 AS
            SELECT day, count(*) AS nfpd
                FROM day_log
                WHERE status = '404 NOT FOUND'
                GROUP BY day;
        """)

    c.execute("""
        CREATE VIEW log_200_404 AS
            SELECT log_200.day, okpd, nfpd
                FROM log_200 FULL OUTER JOIN log_404
                ON log_200.day = log_404.day
                ORDER BY day;
        """)

    c.execute("""
        CREATE VIEW errors_over_1p AS
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

    c.execute("SELECT * FROM errors_over_1p WHERE errp > 1;")
    rows = c.fetchall()

    print("{:^10}  {}".format("Date", "Error Rate"))
    for i, row in enumerate(rows):
        print("{:10}  {}".format(rows[i][0], rows[i][3]))

    db.commit()
    db.close()


if __name__ == '__main__':
    printHeader()
    defineTSNLView()
    printTopArticlesReport()
    printTopAuthorsReport()
    printErrorReport()
