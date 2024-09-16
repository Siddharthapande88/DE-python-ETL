import json
from Extract.extract import extract
from Load.load import load
from Transform.transform import transform
import datetime
from mysql.connector import connect




class mytransform(transform):

    def __init__(self,raw_data):
        super().__init__(raw_data)  #Calling parent class constructor to initiate data input

    def date_format_change(self):
        input_list=[]
        for element in self.input_data['items']:
            creation_ts=datetime.datetime.fromtimestamp(element['creation_date'])
            last_activity_ts=datetime.datetime.fromtimestamp(element['creation_date'])

            element['creation_date'] = creation_ts.strftime('%Y-%m-%d')
            element['last_activity_date'] = last_activity_ts.strftime('%Y-%m-%d')
            mytuple = (element['question_id'], element['title'], element['creation_date'], element['last_activity_date'],json.dumps({'tags': element['tags']}), element['is_answered'])
            input_list.append(mytuple)
        #print(input_list)
        return input_list

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
    tranformed_data=tranform_object.date_format_change()

#   #Data loading

    data_load_object=load(tranformed_data,insert_query,'root','Root@123','localhost')
    data_load_object.get_connection()
    data_load_object.write_into_Db()
    #data_load_object.close_conection()

    conn_1=connect(user=user_name, password=password, host=host)
    db_cursor= conn_1.cursor()

    query="select * from mydatabase.Questions limit 100"
    db_cursor.execute(query)
    result=db_cursor.fetchall()

    print(result)


