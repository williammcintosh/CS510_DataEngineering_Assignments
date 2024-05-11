import time
import psycopg2
import argparse
import csv
from tqdm import tqdm
import psycopg2.extras

# Database configuration
DBname = "postgres"
DBuser = "postgres"
DBpwd = "karlasgremlins"
TableName = 'CensusData'

def row2vals(row):
    for key in row:
        if not row[key]:
            if key in ["CensusTract", "TotalPop", "Men", "Women", "Citizen", "Employed"]:
                row[key] = 0
            elif key in ["Hispanic", "White", "Black", "Native", "Asian", "Pacific", "Income", "IncomeErr", "IncomePerCap", "IncomePerCapErr", "Poverty", "ChildPoverty", "Professional", "Service", "Office", "Construction", "Production", "Drive", "Carpool", "Transit", "Walk", "OtherTransp", "WorkAtHome", "MeanCommute", "PrivateWork", "PublicWork", "SelfEmployed", "FamilyWork", "Unemployment"]:
                row[key] = 0.0
            else:
                row[key] = 'Unknown'
        row['County'] = row['County'].replace('\'', '')
    return (row['CensusTract'], row['State'], row['County'], row['TotalPop'], row['Men'], row['Women'],
            row['Hispanic'], row['White'], row['Black'], row['Native'], row['Asian'], row['Pacific'],
            row['Citizen'], row['Income'], row['IncomeErr'], row['IncomePerCap'], row['IncomePerCapErr'],
            row['Poverty'], row['ChildPoverty'], row['Professional'], row['Service'], row['Office'],
            row['Construction'], row['Production'], row['Drive'], row['Carpool'], row['Transit'],
            row['Walk'], row['OtherTransp'], row['WorkAtHome'], row['MeanCommute'], row['Employed'],
            row['PrivateWork'], row['PublicWork'], row['SelfEmployed'], row['FamilyWork'], row['Unemployment'])

def initialize():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    parser.add_argument("-c", "--createtable", action="store_true")
    parser.add_argument("-o", "--optimize-loading", action="store_true", help="Optimize table loading by delaying constraints/indexes")
    parser.add_argument("-t", "--use-single-transaction", action="store_true", help="Use a single transaction for the entire load process")
    parser.add_argument("--copy-from", action="store_true", help="Use the copy_from method for bulk loading")
    args = parser.parse_args()
    
    return args.datafile, args.createtable, args.optimize_loading, args.use_single_transaction, args.copy_from

def read_data(filename):
    with open(filename, mode="r") as file:
        dr = csv.DictReader(file)
        return list(dr)

def db_connect():
    conn = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
        connect_timeout=3
    )
    return conn

def create_table(conn, optimize_loading=False):
    with conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TableName};")
        sql_command = f"""
            CREATE TABLE {TableName} (
                CensusTract         BIGINT,
                State               TEXT,
                County              TEXT,
                TotalPop            INTEGER,
                Men                 INTEGER,
                Women               INTEGER,
                Hispanic            DECIMAL,
                White               DECIMAL,
                Black               DECIMAL,
                Native              DECIMAL,
                Asian               DECIMAL,
                Pacific             DECIMAL,
                Citizen             INTEGER,
                Income              DECIMAL,
                IncomeErr           DECIMAL,
                IncomePerCap        DECIMAL,
                IncomePerCapErr     DECIMAL,
                Poverty             DECIMAL,
                ChildPoverty        DECIMAL,
                Professional        DECIMAL,
                Service             DECIMAL,
                Office              DECIMAL,
                Construction        DECIMAL,
                Production          DECIMAL,
                Drive               DECIMAL,
                Carpool             DECIMAL,
                Transit             DECIMAL,
                Walk                DECIMAL,
                OtherTransp         DECIMAL,
                WorkAtHome          DECIMAL,
                MeanCommute         DECIMAL,
                Employed            INTEGER,
                PrivateWork         DECIMAL,
                PublicWork          DECIMAL,
                SelfEmployed        DECIMAL,
                FamilyWork          DECIMAL,
                Unemployment        DECIMAL
            """
        if not optimize_loading:
            sql_command += ",\n                PRIMARY KEY (CensusTract)\n            );"
        else:
            sql_command += "\n            );"
        cursor.execute(sql_command)

        if not optimize_loading:
            cursor.execute(f"CREATE INDEX idx_{TableName}_State ON {TableName}(State);")
        conn.commit()
        print(f"Table {TableName} created with optimization: {optimize_loading}.")

def add_constraints_and_indexes(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid
            AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '{TableName}'::regclass
            AND i.indisprimary;
        """)
        pk_exists = cursor.fetchone()[0] > 0

        if not pk_exists:
            cursor.execute(f"""
                ALTER TABLE {TableName} ADD PRIMARY KEY (CensusTract);
            """)
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM pg_indexes
            WHERE tablename = '{TableName}'
            AND indexname = 'idx_{TableName}_State';
        """)
        index_exists = cursor.fetchone()[0] > 0

        if not index_exists:
            cursor.execute(f"""
                CREATE INDEX idx_{TableName}_State ON {TableName}(State);
            """)
        conn.commit()
        print(f"Indexes and constraints added to {TableName}.")

def load_data(conn, rows, use_single_transaction):
    with conn.cursor() as cursor:
        sql_command = f"INSERT INTO {TableName} (CensusTract, State, County, TotalPop, Men, Women, Hispanic, White, Black, Native, Asian, Pacific, Citizen, Income, IncomeErr, IncomePerCap, IncomePerCapErr, Poverty, ChildPoverty, Professional, Service, Office, Construction, Production, Drive, Carpool, Transit, Walk, OtherTransp, WorkAtHome, MeanCommute, Employed, PrivateWork, PublicWork, SelfEmployed, FamilyWork, Unemployment) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        if use_single_transaction:
            print(f"Loading {len(rows)} rows in a single transaction")
            start = time.perf_counter()
            data = [row2vals(row) for row in tqdm(rows, desc="Preparing data")]
            psycopg2.extras.execute_batch(cursor, sql_command, data, page_size=1000)
            conn.commit()
            elapsed = time.perf_counter() - start
            print(f"Finished loading. Elapsed time: {elapsed:.4f} seconds.")
        else:
            print(f"Loading {len(rows)} rows with individual commits")
            start = time.perf_counter()
            for row in tqdm(rows, desc="Inserting rows one by one"):
                data = row2vals(row)
                cursor.execute(sql_command, data)
                conn.commit()
            elapsed = time.perf_counter() - start
            print(f"Finished loading. Elapsed time: {elapsed:.4f} seconds.")

def load_data_with_copy(conn, filename):
    with conn.cursor() as cursor:
        print(f"Starting to load data from {filename} into {TableName} using copy_from.")
        with open(filename, 'r') as f:
            next(f)  # Skip the header row.
            cursor.copy_from(f, TableName.lower(), sep=',', null='')
    conn.commit()
    print(f"Data loaded using copy_from method from {filename}.")

def main():
    start_time = time.perf_counter()  # Start the timer

    datafile, create_table_flag, optimize_loading, use_single_transaction, use_copy_from = initialize()
    conn = db_connect()

    if create_table_flag:
        create_table(conn, optimize_loading)
    else:
        print(f"Notice: Table {TableName} was not recreated; assuming it exists.")

    if use_copy_from:
        load_data_with_copy(conn, datafile)
    else:
        rows = read_data(datafile)
        load_data(conn, rows, use_single_transaction)

    if optimize_loading and create_table_flag:
        add_constraints_and_indexes(conn)

    conn.close()

    total_elapsed = time.perf_counter() - start_time  # End the timer
    print(f"Total elapsed time: {total_elapsed:.4f} seconds.")  # Print the total time taken

if __name__ == "__main__":
    main()
