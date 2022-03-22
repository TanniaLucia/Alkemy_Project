import logging

def create_table(DB, cursor):
    """
    This function creates three tables by looping through the SQL language in the 3 different files
        DB: List of paths where SQL files are located
        cursor: Allows you to interact with the database
    """
    try:
        for i in range(3):
            with open(DB[i], 'r') as file:
                SQL = file.read()
                cursor.execute(SQL)
                logging.info("The table was created successfully")

    except Exception as E:
        logging.error(E)

def update_DB(Data, tabla, conn):
    """
    Update the respective table with the data from the dataframe
        Data: Dataframe with the data
        tabla: Name of the table to update
        conn: Object of type connection
    """
    try:
        Data.to_sql(tabla, conn, if_exists="replace")
        logging.info("The information in the table has been updated")
    except Exception as E:
        logging.error(E)
