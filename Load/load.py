#Write dta to SQLDB
from mysql.connector import connect

class load:
    def __init__(self,tranformed_data,insert_query,user,password,host):
        self.insert_query=insert_query
        self.data=tranformed_data
        self.user=user
        self.password=password
        self.host=host
        self.dbcursor=''
        self.conn=''


    def get_connection(self):

        try:
            self.conn = connect(user=self.user, password=self.password, host=self.host)
        except Exception as e:
            print(f"Connection is not established with error {e}")
        else:
            print(f"Connection is established")
            self.dbcursor=self.conn.cursor()

    def write_into_Db(self):


        try:

            self.dbcursor.executemany(self.insert_query,self.data)
        except Exception as e:
            print(e)
        else:
            self.dbcursor.execute("Commit")
            print("Write successful")

    def close_conection(self):
        self.conn.close_connection(self.dbcursor)


