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
  from text,
  to text,
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
  PRIMARY KEY ((airline, from, to, year, month, day), age, gender, reason, stay, transit, connection)
) WITH CLUSTERING ORDER BY (age DESC, gender DESC, reason DESC, stay DESC, transit DESC, connection DESC);

'''

SELECT_USER_ACCOUNTS = """
    SELECT username, account_number, name, cash_balance
    FROM accounts_by_user
    WHERE username = ?
"""

SELECT_POSITIONS_BY_ACOUNT = """
    SELECT account, symbol, quantity
    FROM positions_by_account
    WHERE account = ?
"""

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_PRINCIPAL_TABLE)



def get_user_accounts(session, username):
    log.info(f"Retrieving {username} accounts")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    for row in rows:
        print(f"=== Account: {row.account_number} ===")
        print(f"- Cash Balance: {row.cash_balance}")

def get_positions_by_account(session, account_number):
    log.info(f"Retrieving positions from {account_number}")
    stmt = session.prepare(SELECT_POSITIONS_BY_ACOUNT)
    rows = session.execute(stmt, [account_number])
    print(f"=== Position of {account_number}")
    for row in rows:
        print(f"- {row.symbol}: {row.quantity}")
