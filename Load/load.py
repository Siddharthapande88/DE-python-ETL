#Write dta to SQLDB
from mysql.connector import connect

class load:
    def __init__(self,tranformed_data,insert_query,user,password,host,port):
        self.insert_query=insert_query
        self.data=tranformed_data
        self.user=user
        self.password=password
        self.host=host



    def load_into_db(self):

        conn=self.get_connection()
        self.write_into_Db(conn)
        #print(dbucrsor)




    def get_connection(self):

        try:
            conn = connect(user=self.user, password=self.password, host=self.host)
            #print(conn)
        except Exception as e:
            print(f"Connection is not established with error {e}")
        else:
            print(f"Connection is established")
            print(conn.is_connected())
            return conn

    def write_into_Db(self,conn):

        try:
            dbcursor=conn.cursor()
            #print(self.data)
            dbcursor.executemany(self.insert_query,self.data)
            dbcursor.execute("Commit")
            #print(dbcursor)
        except Exception as e:
            print(f"Write is unsuccessful due to {e}")
        else:
            print("Write successful")




