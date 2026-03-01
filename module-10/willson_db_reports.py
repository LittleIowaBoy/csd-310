# ---------------------------------------------------------------
# Author:       Zak Nizam
# Group:        4
# Members:      Zak Nizam, Caleb Schumacher
# Date:         March 28, 2026
# Course:       CSD310 - Database Development and Use
# Assignment:   Milestone #3 Willson Financial (Reports)
#
# Purpose:
#   Connects to the willson_financial database and generates
#   three business reports based on the case study questions:
#
#   1) Clients added in the last 6 months
#   2) Average total assets across all clients
#   3) Clients with more than 10 transactions in a single month
#
# Notes:
#   - Reads DB creds from ./root-folder/.env
# ---------------------------------------------------------------

import mysql.connector
from mysql.connector import errorcode
from dotenv import dotenv_values


# Read database credentials from local .env
secrets = dotenv_values(".env")

config = {
    "user": secrets["USER"],
    "password": secrets["PASSWORD"],
    "host": secrets["HOST"],
    "database": secrets["DATABASE"],
    "raise_on_warnings": True
}


def run_report(cursor, title, query, headers=None):
    """
    Executes a query and prints formatted results.
    """
    print(f"\n  -- {title} --")
    cursor.execute(query)
    rows = cursor.fetchall()

    if not rows:
        print("  (no results found)")
        return

    # Print headers if provided
    if headers:
        header_line = " | ".join(headers)
        print(f"  {header_line}")
        print(f"  {'-' * len(header_line)}")

    for row in rows:
        print("  " + " | ".join(str(item) for item in row))


try:
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    # -----------------------------------------------------------
    # REPORT 1
    # Clients added in the last 6 months
    # -----------------------------------------------------------
    report_1 = """
        SELECT
          DATE_FORMAT(client_created_date, '%Y-%m') AS month_added,
          COUNT(*) AS clients_added
        FROM client
        WHERE client_created_date >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
        GROUP BY month_added
        ORDER BY month_added;
    """

    run_report(
        cursor,
        "REPORT 1: CLIENTS ADDED (LAST 6 MONTHS)",
        report_1,
        headers=["month_added", "clients_added"]
    )

    # -----------------------------------------------------------
    # REPORT 2
    # Average total assets across all clients
    # (Deposits - Withdrawals)
    # -----------------------------------------------------------
    report_2 = """
        SELECT
          ROUND(AVG(client_total), 2) AS avg_total_assets
        FROM (
          SELECT
            c.client_id,
            COALESCE(SUM(
              CASE
                WHEN ct.transaction_type = 'Deposit' THEN ct.transaction_amount
                WHEN ct.transaction_type = 'Withdrawal' THEN -ct.transaction_amount
                ELSE 0
              END
            ), 0) AS client_total
          FROM client c
          LEFT JOIN client_transaction ct
            ON c.client_id = ct.client_id
          GROUP BY c.client_id
        ) totals;
    """

    run_report(
        cursor,
        "REPORT 2: AVERAGE TOTAL ASSETS (ALL CLIENTS)",
        report_2,
        headers=["avg_total_assets"]
    )

    # -----------------------------------------------------------
    # REPORT 3
    # Clients with more than 10 transactions in a month
    # -----------------------------------------------------------
    report_3 = """
        SELECT
          c.client_id,
          CONCAT(c.client_first_name, ' ', c.client_last_name) AS client_name,
          DATE_FORMAT(ct.transaction_date, '%Y-%m') AS txn_month,
          COUNT(*) AS transaction_count
        FROM client_transaction ct
        JOIN client c ON ct.client_id = c.client_id
        GROUP BY c.client_id, client_name, txn_month
        HAVING COUNT(*) > 10
        ORDER BY transaction_count DESC;
    """

    run_report(
        cursor,
        "REPORT 3: CLIENTS WITH >10 TRANSACTIONS IN A MONTH",
        report_3,
        headers=["client_id", "client_name", "txn_month", "transaction_count"]
    )

    input("\n  Press any key to continue...")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Invalid username or password.")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist.")
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