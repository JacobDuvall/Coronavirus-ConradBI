import pyodbc
import pandas as pd
import os


# Deletes all rows from table
def delete_county_table_database():
    # Connect to database
    conn = connect_database()

    # Create database cursor
    cursor = conn.cursor()

    # Execute insertion and commit to database
    cursor.execute("CALL coronavirus.clear_county_totals;")
    conn.commit()

    # Close connection
    cursor.close()
    disconnect_database(conn)


# Inserts into database
def insert_database(query):

    # Connect to database
    conn = connect_database()

    # Create database cursor
    cursor = conn.cursor()

    # Execute insertion and commit to database
    cursor.execute(query)
    conn.commit()

    # Close connection
    cursor.close()
    disconnect_database(conn)


def select_database(query):

    # Connect to database
    conn = connect_database()

    # Read query into table
    table = pd.read_sql_query(query, conn)

    # Close connection
    disconnect_database(conn)

    return table


# Disconnect from database
def disconnect_database(conn):

    conn.close()


def insert_counties_to_database(counties):

    # Connect to database
    conn = connect_database()

    # Create database cursor
    cursor = conn.cursor()

    for index, row in counties.iterrows():
        query = "INSERT INTO county_totals (date, " \
                "county, state, fips, cases, deaths) " \
                "VALUES ('{0}', '{1}', '{2}', '{3}', {4}, {5})".format(
            row['date'],
            row['county'].replace("'", ""), row['state'], row['fips'],
            row['cases'], row['deaths']
        )
        cursor.execute(query)

    conn.commit()
    cursor.close()
    disconnect_database(conn)


def bulk_insert_county_csv():
    conn = connect_database()
    cursor = conn.cursor()
    sql = "DELETE FROM coronavirus.a_county_totals"
    cursor.execute(sql)
    conn.commit()
    sql = "LOAD DATA LOCAL INFILE " \
          "'C:\\\\Users\\\\jdale\\\\OneDrive\\\\coronavirus\\\\Coronavirus-ConradBI\\\\datasets\\\\county_data.csv' " \
          "INTO TABLE a_county_totals " \
          "FIELDS TERMINATED BY ',' " \
          "LINES TERMINATED BY '\n' " \
          "IGNORE 1 ROWS"
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    disconnect_database(conn)


def bulk_insert_state_csv():
    conn = connect_database()
    cursor = conn.cursor()
    sql = "DELETE FROM coronavirus.a_state_totals"
    cursor.execute(sql)
    conn.commit()
    sql = "LOAD DATA LOCAL INFILE " \
          "'C:\\\\Users\\\\jdale\\\\OneDrive\\\\coronavirus\\\\Coronavirus-ConradBI\\\\datasets\\\\state_data.csv' " \
          "INTO TABLE a_state_totals " \
          "FIELDS TERMINATED BY ',' " \
          "LINES TERMINATED BY '\n' " \
          "IGNORE 1 ROWS"
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    disconnect_database(conn)


def bulk_insert_weather_csv():
    conn = connect_database()
    cursor = conn.cursor()
    sql = "LOAD DATA LOCAL INFILE " \
          "'C:\\\\Users\\\\jdale\\\\OneDrive\\\\coronavirus\\\\Coronavirus-ConradBI\\\\datasets\\\\daily_weather.csv' " \
          "INTO TABLE a_county_weather " \
          "FIELDS TERMINATED BY ',' " \
          "LINES TERMINATED BY '\n' " \
          "IGNORE 1 ROWS"
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    disconnect_database(conn)

def connect_database():
    # Connect to database and return connection
    return pyodbc.connect("Driver={MySQL ODBC 5.3 ANSI Driver};"
                          "Server=" + os.environ['MY_HOSTNAME'] +
                          ";Database=coronavirus"
                          ";UID=" + os.environ['MY_USERNAME'] +
                          ";PWD=" + os.environ['MY_PASSWORD'] +
                          ';enable_local_infile=1;'
                          )