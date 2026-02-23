# ---------------------------------------------------------------
# Author:       Zak Nizam
# Group:        4
# Members:      Zak Nizam, Caleb Schumacher
# Date:         February 20, 2026
# Course:       CSD310 - Database Development and Use
# Assignment:   Milestone #2 - Willson Financial (DB Init + Display)
#
# Purpose:
#   Creates/initializes the database, inserts sample data,
#   and then prints the contents of each table.
#
# Notes:
#   - Reads DB creds from ./root-folder/.env
# ---------------------------------------------------------------

import re

import mysql.connector
from mysql.connector import errorcode
from dotenv import dotenv_values


secrets = dotenv_values("./.env")

def required_secret(key: str) -> str:
  value = secrets.get(key)
  if not value:
    raise ValueError(f"Missing .env key: {key}")
  return value


database_name = required_secret("DATABASE")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database_name):
    raise ValueError("DATABASE in .env must be a valid MySQL identifier.")

config = {
  "user": required_secret("USER"),
  "password": required_secret("PASSWORD"),
  "host": required_secret("HOST"),
    "raise_on_warnings": False,
}

init_statements = [
    f"CREATE DATABASE IF NOT EXISTS `{database_name}`",
    f"USE `{database_name}`",
    "DROP TABLE IF EXISTS appointment",
    "DROP TABLE IF EXISTS client_transaction",
    "DROP TABLE IF EXISTS employee",
    "DROP TABLE IF EXISTS client",
    """
    CREATE TABLE client (
      client_id INT AUTO_INCREMENT PRIMARY KEY,
      client_first_name VARCHAR(50) NOT NULL,
      client_last_name  VARCHAR(50) NOT NULL,
      client_email      VARCHAR(100) NOT NULL UNIQUE,
      client_created_date DATE NOT NULL
    )
    """,
    """
    CREATE TABLE employee (
      employee_id INT AUTO_INCREMENT PRIMARY KEY,
      employee_first_name VARCHAR(50) NOT NULL,
      employee_last_name  VARCHAR(50) NOT NULL,
      employee_role       VARCHAR(50) NOT NULL
    )
    """,
    """
    CREATE TABLE client_transaction (
      transaction_id INT AUTO_INCREMENT PRIMARY KEY,
      transaction_date DATE NOT NULL,
      transaction_amount DECIMAL(15,2) NOT NULL,
      transaction_type VARCHAR(50) NOT NULL,
      client_id INT NOT NULL,
      CONSTRAINT fk_client_transaction_client
        FOREIGN KEY (client_id)
        REFERENCES client(client_id)
        ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE appointment (
      appointment_id INT AUTO_INCREMENT PRIMARY KEY,
      appointment_date DATETIME NOT NULL,
      client_id INT NOT NULL,
      employee_id INT NOT NULL,
      CONSTRAINT fk_appointment_client
        FOREIGN KEY (client_id)
        REFERENCES client(client_id)
        ON DELETE CASCADE,
      CONSTRAINT fk_appointment_employee
        FOREIGN KEY (employee_id)
        REFERENCES employee(employee_id)
        ON DELETE CASCADE
    )
    """,
    """
    INSERT INTO client (client_first_name, client_last_name, client_email, client_created_date)
    VALUES
      ('Ava',    'Martinez', 'ava.martinez@email.com',    '2025-10-15'),
      ('Mason',  'Reed',     'mason.reed@email.com',      '2025-11-04'),
      ('Sophia', 'Nguyen',   'sophia.nguyen@email.com',   '2025-12-12'),
      ('Ethan',  'Carter',   'ethan.carter@email.com',    '2026-01-05'),
      ('Olivia', 'Bennett',  'olivia.bennett@email.com',  '2026-01-18'),
      ('Noah',   'Parker',   'noah.parker@email.com',     '2026-02-01')
    """,
    """
    INSERT INTO employee (employee_first_name, employee_last_name, employee_role)
    VALUES
      ('Jake',    'Willson',   'Advisor'),
      ('Ned',     'Willson',   'Advisor'),
      ('Phoenix', 'Two Star',  'Office Administrator'),
      ('June',    'Santos',    'Compliance Manager'),
      ('Morgan',  'Lee',       'Advisor'),
      ('Taylor',  'Brooks',    'Client Services')
    """,
    """
    INSERT INTO client_transaction (transaction_date, transaction_amount, transaction_type, client_id)
    VALUES
      ('2026-01-02',  5000.00, 'Deposit',    1),
      ('2026-01-05',   250.00, 'Withdrawal', 1),
      ('2026-01-06',   400.00, 'Withdrawal', 1),
      ('2026-01-07',  1200.00, 'Deposit',    1),
      ('2026-01-08',   150.00, 'Withdrawal', 1),
      ('2026-01-09',   900.00, 'Deposit',    1),
      ('2026-01-10',   100.00, 'Withdrawal', 1),
      ('2026-01-11',   300.00, 'Withdrawal', 1),
      ('2026-01-12',   700.00, 'Deposit',    1),
      ('2026-01-13',   200.00, 'Withdrawal', 1),
      ('2026-01-14',  1100.00, 'Deposit',    1),
      ('2026-01-15',  2500.00, 'Deposit',    2),
      ('2026-01-18',   500.00, 'Withdrawal', 3),
      ('2026-02-02',  3500.00, 'Deposit',    4),
      ('2026-02-10',   250.00, 'Withdrawal', 5),
      ('2026-02-12',   900.00, 'Deposit',    6)
    """,
    """
    INSERT INTO appointment (appointment_date, client_id, employee_id)
    VALUES
      ('2026-02-10 09:00:00', 1, 1),
      ('2026-02-11 10:30:00', 2, 2),
      ('2026-02-12 14:00:00', 3, 5),
      ('2026-02-13 13:15:00', 4, 6),
      ('2026-02-14 11:00:00', 1, 4),
      ('2026-02-15 15:30:00', 5, 3)
    """,
]


def initialize_database(cursor) -> None:
    for statement in init_statements:
        cursor.execute(statement)


def show_table(cursor, table_name: str, title: str, order_column: str) -> None:
    print(f"\n  -- {title} --")
    cursor.execute(f"SELECT * FROM `{table_name}` ORDER BY `{order_column}`;")
    rows = cursor.fetchall()

    if not rows:
        print("  (no records found)")
        return

    for row in rows:
        print(f"  {row}")


try:
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    initialize_database(cursor)
    db.commit()

    show_table(cursor, "client", "CLIENT TABLE", "client_id")
    show_table(cursor, "employee", "EMPLOYEE TABLE", "employee_id")
    show_table(cursor, "client_transaction", "TRANSACTION TABLE", "transaction_id")
    show_table(cursor, "appointment", "APPOINTMENT TABLE", "appointment_id")

    input("\n  Press Enter to continue...")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("  Invalid username or password.")
    else:
        print(err)

finally:
    try:
        cursor.close()
    except Exception:
        pass
    try:
        db.close()
    except Exception:
        pass
