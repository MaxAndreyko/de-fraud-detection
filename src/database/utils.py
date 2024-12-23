from psycopg2.extensions import connection as Connection

def execute_ddl_script(filepath: str, connection: Connection) -> None:
    """
    Execute a DDL script from a specified file.

    Parameters
    ----------
    filepath : str
        The path to the SQL file containing the DDL commands.
    connection : psycopg2.extensions.connection
        A psycopg2 connection object to the PostgreSQL database.

    Returns
    -------
    None
        Executes the SQL commands in the file. If an error occurs,
        it raises an exception.
    
    Examples
    --------
    >>> conn = psycopg2.connect(dbname='your_db', user='your_user', password='your_password', host='localhost')
    >>> execute_ddl_script('path/to/your_script.sql', conn)
    """
    with connection.cursor() as cursor:
        with open(filepath, 'r') as sql_file:
            sql_script = sql_file.read()
            cursor.execute(sql_script)
        connection.commit()
