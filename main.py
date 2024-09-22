import json
from Extract.extract import extract
from Load.load import load
from Transform.transform import transform
import datetime
from mysql.connector import connect




class mytransform(transform):

    def __init__(self,raw_data):
        super().__init__(raw_data)  #Calling parent class constructor to initiate data input

    def transformation(self):
        input_list=[]
        for element in self.input_data['items']:
            #input_ts=datetime.datetime.fromtimestamp(element['creation_date'])
            element['creation_date']=self.date_format_change(element['creation_date'])

            #input_ts=datetime.datetime.fromtimestamp(element['last_activity_date'])
            element['last_activity_date'] =self.date_format_change(element['last_activity_date'])

            mytuple = (element['question_id'], element['title'], element['creation_date'], element['last_activity_date'],json.dumps({'tags': element['tags']}), element['is_answered'])
            input_list.append(mytuple)
        #print(input_list)
        return input_list

    def date_format_change(self,input_ts):

        #print(input_ts.strftime('%Y-%m-%d'))
        return datetime.datetime.fromtimestamp(input_ts).strftime('%Y-%m-%d')

if __name__ == "__main__":


    url='https://api.stackexchange.com/2.3/questions?order=desc&sort=activity&site=stackoverflow'
    insert_query="""Insert into mydatabase.Questions(question_id,title,question_creation_date,last_activity_date,question_tags,is_answered)
     values (%s,%s,%s,%s,%s,%s)
    """
    user_name='root'
    password='Root@123'
    host='localhost'

   # Data Ingestion
    extract_object=extract(url)
    raw_data=extract_object.ingest()

    # Data Transformation

    tranform_object=mytransform(raw_data)
    tranformed_data=tranform_object.transformation()
#   #Data loading
    #print(tranformed_data)
    data_load_object=load(tranformed_data,insert_query,'root','Root@123','localhost',3306)
    #data_load_object.get_connection()
    #data_load_object.write_into_Db()
    data_load_object.load_into_db()

    conn_1=connect(user=user_name, password=password, host=host)
    db_cursor= conn_1.cursor()

    query="select * from mydatabase.Questions limit 100"
    db_cursor.execute(query)
    result=db_cursor.fetchall()

    print(result)


