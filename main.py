import os
import mysql.connector
import logging
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# Connect to database
def monthly_script():
    mydb = mysql.connector.connect (
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DATABASE')
    )
    # Create db connection buffers
    curA = mydb.cursor(buffered=True)
    curB = mydb.cursor(buffered=True)
    curC = mydb.cursor(buffered=True)
    curD = mydb.cursor(buffered=True)

    # Define SQL statements
    get_virtual_accounts = (
        "SELECT id AS vaccount, starting_balance, current_balance, amount FROM virtual_account"
    )

    sum_virtual_account = (
        "SELECT SUM(amount) AS Total FROM transactions WHERE virtual_account = %s"
    )

    update_balance = (
        "UPDATE virtual_account SET starting_balance = %s, current_balance = %s WHERE id = %s"
    )

    copy_transactions = (
        "INSERT INTO transactionshistory (user, virtual_account, amount, timestamp, tofrom, description) SELECT user, virtual_account, amount, timestamp, tofrom, description FROM transactions"
    )

    drop_transactions = (
        "TRUNCATE TABLE transactions"
    )

    # List all virtual accounts
    curA.execute(get_virtual_accounts)

    # Store as an array
    vaccount_array = curA.fetchall()

    # Loop through each item of the array
    for (vaccount, starting_balance, current_balance, amount) in vaccount_array:
        curB.execute(sum_virtual_account, (vaccount,))
        totals_array = curB.fetchall()
        for (Total) in totals_array:
            Total = (Total[0])
            try:
                new_balance = (Total + starting_balance + amount)
            except TypeError:
                new_balance = (starting_balance + amount)
            if (new_balance - amount) != current_balance:
                logging.warning("Balance error for virtual account")
            curC.execute(update_balance, (new_balance, new_balance, vaccount))
            mydb.commit()
            logging.info(curC.rowcount, "record(s) affected")

    copysuccess = False
    try:
        curD.execute(copy_transactions)
        mydb.commit()
        copysuccess = True
    except mysql.connector.Error as e:
        logging.warning("Error:", e)
    if copysuccess:
        curD.execute(drop_transactions)
        mydb.commit()
        logging.info("success!!!")

# Daily script that just makes connection to DB for test purposes
def daily_script():

    # Connect to DB
    mydb = mysql.connector.connect (
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DATABASE')
    )
    # Create db connection buffers
    curA = mydb.cursor(buffered=True)

    # Get total balance for all virtual accounts that we're interested in (matching physical acct 51)
    sum_total_balance = (
        "SELECT SUM(current_balance) FROM virtual_account WHERE physical_account = '51'"
    )

    curA.execute(sum_total_balance)

    total_balance = curA.fetchall()

    print("Total balance:", total_balance)


today = datetime.now()
if today.day == 1:
    print("Run script today!")
    print(today.day)
    monthly_script()
else:
    print("Not today...")
    daily_script()
    print(today.day)