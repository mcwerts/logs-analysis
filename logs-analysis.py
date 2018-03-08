#!/usr/bin/env python
#
# logs-analysis.py --
#

import psycopg2

SQL_TOP3_ARTICLES_QUERY = """
    SELECT * FROM articles_by_pop ORDER BY num LIMIT 3;
    """

def connect():
    """Connect to the PostgreSQL database.  Returns a database connection."""
    return psycopg2.connect("dbname=news")

def defineViews():
    print("Define Views")
    db = connect()
    c = db.cursor()

    # The order of these view operations is IMPORTANT
    # top 3 articles and top authors
    c.execute("DROP VIEW IF EXISTS title_slug_name_log;")
    c.execute("DROP VIEW IF EXISTS log_article_200;")
    c.execute("DROP VIEW IF EXISTS title_slug_name;")

    # errors

    # create the views
    # Top 3 articles and top authors

    #time not needed?
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
    print("\n[1] TOP 3 ARTICLES")
    print("===================")
    db = connect()
    c = db.cursor()
    c.execute("""
        SELECT title, count(*) AS views
        FROM title_slug_name_log
        GROUP BY title
        ORDER BY views DESC
        LIMIT 3;""")
    rows = c.fetchall();
    db.commit()
    db.close
    for i, row in enumerate(rows):
        print(rows[i])


def printTopAuthorsReport():
    print("\n[2] TOP AUTHORS")
    print("================")
    db = connect()
    c = db.cursor()
    c.execute("""
        SELECT name, count(*) AS views
        FROM title_slug_name_log
        GROUP BY name
        ORDER BY views DESC;""")
    rows = c.fetchall();
    db.commit()
    db.close
    for i, row in enumerate(rows):
        print(rows[i])

def printErrorReport():
    print("\n[3] DAYS WITH ERRORS OVER 1% THRESHOLD")
    print("=======================================")
    db = connect()
    c = db.cursor()

    # Errors
    c.execute("DROP VIEW IF EXISTS errors_over_1p;")
    c.execute("DROP VIEW IF EXISTS log_200_404;")
    c.execute("DROP VIEW IF EXISTS log_200;")
    c.execute("DROP VIEW IF EXISTS log_404;")
    c.execute("DROP VIEW IF EXISTS day_log;")

    c.execute("""
        CREATE VIEW day_log AS
            SELECT to_char(time, 'YYYY-MM-DD') AS day, status FROM log;
        """)

#    create view log_get as
#        select day, count(*) as gets from day_log group by day;

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
    for i, row in enumerate(rows):
        print(rows[i])

    db.commit()
    db.close()

def printHeader():
    print("ACTIVITY REPORT FOR PERIOD DATE1 TO DATE2")
    db = connect()
    c = db.cursor()

    c.execute("SELECT min(time) FROM log;")
    rows = c.fetchall()
    print(rows)

    c.execute("SELECT max(time) FROM log;")
    rows = c.fetchall()
    print(rows)

    db.commit()
    db.close()


if __name__ == '__main__':
    defineViews()
    printHeader()
    printTopArticlesReport()
    printTopAuthorsReport()
    printErrorReport()
    print("\n*** END OF REPORT ***")
