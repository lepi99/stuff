__author__ = 'rfernandes'
from pandas import read_sql_table
from sqlalchemy import create_engine


if __name__ == "__main__":
    server   = "PMSISQL06"
    database = "Smiths_Group_IBP"
    table    = "IBP_Demand_dates"
    engine = create_engine("mssql+pymssql://" + server + "/" + database)
    print("Before read_sql_table...")
    df = read_sql_table(table, con=engine)
    print(df)