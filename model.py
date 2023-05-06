#!/usr/bin/env python3
import logging

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_PRINCIPAL_TABLE = '''
CREATE TABLE IF NOT EXISTS airport_wait_time (
  airline text,
  de text,
  hacia text,
  day int,
  month int,
  year int,
  age int,
  gender text,
  reason text,
  stay int,
  transit text,
  connection text,
  wait int,
  PRIMARY KEY ((airline, de, hacia, year, month, day), age, gender, reason, stay, transit, connection)
) WITH CLUSTERING ORDER BY (age DESC, gender DESC, reason DESC, stay DESC, transit DESC, connection DESC);

'''

SELECT_ALL = '''
    SELECT * FROM airport_wait_time;
    '''

SELECT_BY_AIRLINE = '''
    SELECT * FROM airport_wait_time WHERE airline = ? ;
    '''

SELECT_BY_AIRLINE_WAIT = '''
    SELECT * FROM airport_wait_time WHERE airline = ? AND wait = ?;
    '''

SELECT_BY_WAIT_LESS_0 = '''
    SELECT * FROM airport_wait_time WHERE wait <= ?;
    '''
SELECT_BY_WAIT_MORE_0 = '''
    SELECT * FROM airport_wait_time WHERE wait >= ?;
    '''

SELECT_BY_MONTH_YEAR = '''
    SELECT * FROM airport_wait_time WHERE month = ? AND year = ?;
    '''

SELECT_BY_FROM_TO = '''
    SELECT * FROM airport_wait_time WHERE from = ? AND to = ? ;
    '''

SELECT_BY_FROM_TO_WAIT = '''
    SELECT * FROM airport_wait_time WHERE from = ? AND to = ? AND wait = ? ;
    '''

SELECT_BY_STAY_CONNECTION = '''
    SELECT * FROM airport_wait_time WHERE stay = ? AND connection = ?;
    '''


SELECT_BY_AIRLINE_FROM = '''
    SELECT * FROM airport_wait_time WHERE airline = ? AND from = ?;
    '''

SELECT_BY_TRANSIT_WAIT = '''
    SELECT * FROM airport_wait_time WHERE transit = ? AND wait > ?;
    '''

SELECT_BY_FROM_TO_MONTH = '''
    SELECT * FROM airport_wait_time WHERE from = ? AND to = ? AND month = ?;   
    '''

#selects especiales----------------------------------

#UN DIA MES AÑO ESPECIFICO
SELECT_BY_PERCENTAJE_DATE = '''
    SELECT COUNT()100.0/SUM(COUNT(*)) OVER() AS percentage
    FROM airport_wait_time
    WHERE day = ? AND month = ? AND year = ?;
    '''

#VUELOS EN UN DIA ESPECIFICO
SELECT_BY_PERCENTAJE_DAY = '''
    SELECT COUNT()100.0/SUM(COUNT(*)) OVER() AS percentage
    FROM airport_wait_time
    WHERE day = ?;
    '''

#VUELOS EN DETERMINADO TIEMPO (LEES AND MORE)
SELECT_BY_PERCENTAJE__LESS_WAIT = '''
    SELECT COUNT()100.0/SUM(COUNT(*)) OVER() AS percentage
    FROM airport_wait_time
    WHERE wait <= ?;
    '''
SELECT_BY_PERCENTAJE__MORE_WAIT = '''
    SELECT COUNT()100.0/SUM(COUNT(*)) OVER() AS percentage
    FROM airport_wait_time
    WHERE wait >= ?;
    '''

#VUELOS QUE VAN A UN AEROPUERTO Y UN (DIA, MES, AÑO) ESPECIFICO
SELECT_BY_PERCENTAJE_TO_DAY = '''
    SELECT (COUNT() 100 / (SELECT COUNT(*) FROM airport_wait_time WHERE day = ?)) AS percentage FROM airport_wait_time WHERE to = ? AND day = ?;
    '''

SELECT_BY_PERCENTAJE_TO_MONTH = '''
    SELECT (COUNT() 100 / (SELECT COUNT(*) FROM airport_wait_time WHERE month = ?)) AS percentage FROM airport_wait_time WHERE to = ? AND month = ?;
    '''

SELECT_BY_PERCENTAJE_TO_YEAR = '''
    SELECT (COUNT() 100 / (SELECT COUNT(*) FROM airport_wait_time WHERE year = ?)) AS percentage FROM airport_wait_time WHERE to = ? AND year = ?;
    '''

# VUELOS POR CONEXION
SELECT_BY_PERCENTAJE_CONNECTION_TRUE = '''
    SELECT (COUNT() 100 / (SELECT COUNT(*) FROM airport_wait_time WHERE connection = 'True')) AS percentage FROM airport_wait_time WHERE connection = 'True';
    '''

SELECT_BY_PERCENTAJE_CONNECTION_FALSE = '''
    SELECT (COUNT() 100 / (SELECT COUNT(*) FROM airport_wait_time WHERE connection = 'False')) AS percentage FROM airport_wait_time WHERE connection = 'False';
    '''

# VUELOS DE UNA AEROLINEA EN UNA FECHA ESPECIFICA
SELECT_BY_PERCENTAJE__AIRLINE_DATE = '''
    SELECT (COUNT() 100 / (SELECT COUNT(*) FROM airport_wait_time WHERE airline = ? AND day = ? AND month = ? AND year = ?)) AS percentage FROM airport_wait_time WHERE airline = ? AND day = ? AND month = ? AND year = ?;
    '''


def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_PRINCIPAL_TABLE)

'''

def get_user_accounts(session, username):
    log.info(f"Retrieving {username} accounts")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    for row in rows:
        print(f"=== Account: {row.account_number} ===")
        print(f"- Cash Balance: {row.cash_balance}")
'''

def select_all(session):
    log.info("Retrieving all airport wait times")
    stmt = session.prepare(SELECT_ALL)
    rows = session.execute(stmt)
    print(f"=== All airport flight")
    print("\n")
    for row in rows:
        print(row)

def select_by_airline(session, airline):
    log.info(f"Retrieving airport wait times for airline {airline}")
    stmt = session.prepare(SELECT_BY_AIRLINE)
    rows = session.execute(stmt, [airline])
    print(f"=== Airport wait times for airline {airline}")
    print("\n")
    for row in rows:
        print(row)

def select_by_airline_wait(session, airline, wait_time):
    log.info(f"Retrieving airport wait times for airline {airline} and wait time {wait_time}")
    stmt = session.prepare(SELECT_BY_AIRLINE_WAIT)
    rows = session.execute(stmt, [airline, wait_time])
    print(f"=== Airport wait times for airline {airline} and wait time {wait_time}")
    print("\n")
    for row in rows:
        print(row)

def select_by_wait_less_0(session, wait_time):
    log.info(f"Retrieving airport wait times with wait time less than or equal to {wait_time}")
    stmt = session.prepare(SELECT_BY_WAIT_LESS_0)
    rows = session.execute(stmt, [wait_time])
    print(f"=== Airport wait times with wait time less than or equal to {wait_time}")
    print("\n")
    for row in rows:
        print(row)

def select_by_wait_more_0(session, wait_time):
    log.info(f"Retrieving airport wait times with wait time greater than or equal to {wait_time}")
    stmt = session.prepare(SELECT_BY_WAIT_MORE_0)
    rows = session.execute(stmt, [wait_time])
    print(f"=== Airport wait times with wait time greater than or equal to {wait_time}")
    print("\n")
    for row in rows:
        print(row)

def select_by_month_year(session, month, year):
    log.info(f"Retrieving airport wait times for month {month} and year {year}")
    stmt = session.prepare(SELECT_BY_MONTH_YEAR)
    rows = session.execute(stmt, [month, year])
    print(f"=== Airport wait times for month {month} and year {year}")
    print("\n")
    for row in rows:
        print(row)

def select_by_from_to(session, origin, destination):
    log.info(f"Retrieving airport wait times for origin {origin} and destination {destination}")
    stmt = session.prepare(SELECT_BY_FROM_TO)
    rows = session.execute(stmt, [origin, destination])
    print(f"=== Airport wait times for origin {origin} and destination {destination}")
    print("\n")
    for row in rows:
        print(row)

def select_by_from_to_wait(session, origin, destination, wait_time):
    log.info(f"Retrieving airport wait times for origin {origin}, destination {destination} and wait time {wait_time}")
    stmt = session.prepare(SELECT_BY_FROM_TO_WAIT)
    rows = session.execute(stmt, [origin, destination, wait_time])
    print(f"=== Airport wait times for origin {origin}, destination {destination} and wait time {wait_time}")
    print("\n")
    for row in rows:
        print(row)

def select_by_stay_connection(session, stay, connection):
    log.info(f"Retrieving airport wait times for stay {stay} and connection {connection}")
    stmt = session.prepare(SELECT_BY_STAY_CONNECTION)
    rows = session.execute(stmt, [stay, connection])
    print(f"=== Airport wait times for stay {stay} and connection {connection}")
    print("\n")
    for row in rows:
        print(row)

def select_by_airline_from(session, airline, origin):
    log.info(f"Retrieving airport wait times for airline {airline} and origin {origin}")
    stmt = session.prepare(SELECT_BY_AIRLINE_FROM)
    rows = session.execute(stmt, [airline, origin])
    print(f"=== Airport wait times for airline {airline} and origin {origin}")
    print("\n")
    for row in rows:
        print(row)

def select_by_transit_wait(session, transit, wait_time):
    log.info(f"Retrieving airport wait times for transit {transit} and wait time greater than {wait_time}")
    stmt = session.prepare(SELECT_BY_TRANSIT_WAIT)
    rows = session.execute(stmt, [transit, wait_time])
    print(f"=== Airport wait times for transit {transit} and wait time greater than {wait_time}")
    print("\n")
    for row in rows:
        print(row)

def select_by_from_to_month(session, origin, destination, month):
    log.info(f"Retrieving airport wait time for flights from {origin} to {destination} in {month}")
    stmt = session.prepare(SELECT_BY_FROM_TO_MONTH)
    rows = session.execute(stmt, [origin, destination, month])
    print(f"=== Airport wait time for flights from {origin} to {destination} in {month}")
    print("\n")
    for row in rows:
        print(row)

#Funciones especiales

def select_by_percentaje_date(session, day, month, year):
    log.info(f"Retrieving percentage by date: {day}/{month}/{year}")
    stmt = session.prepare(SELECT_BY_PERCENTAJE_DATE)
    rows = session.execute(stmt, [day, month, year])
    print(f"Percentage by date: {day}/{month}/{year}")
    print("\n")
    for row in rows:
        print(f"Percentage: {row.percentage:.2f}%")

def select_by_percentaje_day(session, day):
    log.info(f"Retrieving percentage by day: {day}")
    stmt = session.prepare(SELECT_BY_PERCENTAJE_DAY)
    rows = session.execute(stmt, [day])
    print(f"Percentage by day: {day}")
    print("\n")
    for row in rows:
        print(f"Percentage: {row.percentage:.2f}%")

def select_by_percentaje_less_wait(session, wait):
    log.info(f"Retrieving percentage by wait time less or equal to {wait} minutes")
    stmt = session.prepare(SELECT_BY_PERCENTAJE__LESS_WAIT)
    rows = session.execute(stmt, [wait])
    print(f"Percentage by wait time less or equal to {wait} minutes")
    print("\n")
    for row in rows:
        print(f"Percentage: {row.percentage:.2f}%")

def select_by_percentaje_more_wait(session, wait):
    log.info(f"Retrieving percentage by wait time more or equal to {wait} minutes")
    stmt = session.prepare(SELECT_BY_PERCENTAJE__MORE_WAIT)
    rows = session.execute(stmt, [wait])
    print(f"Percentage by wait time more or equal to {wait} minutes")
    print("\n")
    for row in rows:
        print(f"Percentage: {row.percentage:.2f}%")

def select_by_percentaje_to_day(session, day, airport):
    log.info(f"Retrieving percentage by airport {airport} on day {day}")
    stmt = session.prepare(SELECT_BY_PERCENTAJE_TO_DAY)
    rows = session.execute(stmt, [day, airport, day])
    print(f"Percentage by airport {airport} on day {day}")
    print("\n")
    for row in rows:
        print(f"Percentage: {row.percentage:.2f}%")

def select_by_percentaje_to_month(session, month, airport):
    log.info(f"Retrieving percentage by airport {airport} on month {month}")
    stmt = session.prepare(SELECT_BY_PERCENTAJE_TO_MONTH)
    rows = session.execute(stmt, [month, airport, month])
    print(f"Percentage by airport {airport} on month {month}")
    print("\n")
    for row in rows:
        print(f"Percentage: {row.percentage:.2f}%")

def select_by_percentaje_to_year(session, year, airport):
    log.info(f"Retrieving percentage by airport {airport} on year {year}")
    stmt = session.prepare(SELECT_BY_PERCENTAJE_TO_YEAR)
    rows = session.execute(stmt, [year, airport, year])
    print(f"Percentage by airport {airport} on year {year}")
    print("\n")
    for row in rows:
        print(f"Percentage: {row.percentage:.2f}%")

def select_by_percentaje_connection_true(session):
    log.info("Retrieving percentage of flights with connections")
    stmt = session.prepare(SELECT_BY_PERCENTAJE_CONNECTION_TRUE)
    rows = session.execute(stmt)
    print(f"Percentage of flights with connections")
    print("\n")
    for row in rows:
        print(f"Percentage: {row.percentage:.2f}%")

def select_by_percentaje_connection_false(session):
    log.info("Retrieving percentage of flights without connections")
    stmt = session.prepare(SELECT_BY_PERCENTAJE_CONNECTION_FALSE)
    rows = session.execute(stmt)
    print(f"Percentage of flights without connections")
    print("\n")
    for row in rows:
        print(f"Percentage: {row.percentage:.2f}%")

def select_by_percentaje_airline_date(session, airline, day, month, year):
    log.info(f"Retrieving percentage of {airline} flights on {day}/{month}/{year}")
    stmt = session.prepare(SELECT_BY_PERCENTAJE__AIRLINE_DATE)
    rows = session.execute(stmt, [airline, day, month, year, airline, day, month, year])
    print(f"Percentage of {airline} flights on {day}/{month}/{year}")
    print("\n")
    for row in rows:
         print(f"Percentage: {row.percentage:.2f}%")

