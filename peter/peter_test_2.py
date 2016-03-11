# -*- coding: utf-8 -*-
"""
TEST
"""


from pandas import read_sql,read_sql_table
from sqlalchemy import create_engine


def sql_server_connection(server, database):
    connection_string = "mssql+pymssql://" + server + "/" + database

    con = create_engine(connection_string)
    return con


if __name__ == "__main__":
    connection = sql_server_connection("pmsisql02", "Georgia_Pacific")
    # connection = sql_server_connection("PMSISQL06", "Smiths_Group_IBP")
    #df = read_sql_table('IBP_Demand_dates', con=connection)
    # df = read_sql("select top 10 * from [dbo].[IBP_Demand_dates]", connection)
    df = read_sql("select top 10 * from [dbo].[eventsraw]", connection)
    print(df)
