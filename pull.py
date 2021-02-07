import requests
import pymysql
import base64
import os
from urllib.request import Request, urlopen 

# mysql credentials

mydb = pymysql.connect(
    host="127.0.0.1",
    user="user",
    passwd="password",
    database="queue"
)

# website credentials

password ='password'
user = 'user'
credentials = ('%s:%s' % (user, password))
encoded_credentials = base64.b64encode(credentials.encode('ascii'))

mycursor_all = mydb.cursor()
sql_all = "SELECT * FROM `group_name` WHERE status = '0'"
mycursor_all.execute(sql_all)
myresult_all = mycursor_all.fetchall()

for y in myresult_all:

    # select a group name at random to avoid request collisions when running the script in parallel

    mycursor = mydb.cursor()
    sql = "SELECT * FROM `group_name` WHERE status = '0' ORDER BY RAND() LIMIT 1"
    mycursor.execute(sql)
    myresult = mycursor.fetchall()

    for x in myresult:
        file_name = x[0]
        print(file_name)
        file_status = x[1]
        filepath = '/home/ubuntu/file-retriever/groups/' + x[0]
        print(filepath)

        # update the status of the group file to "p" for "pending"

        update_status = "UPDATE `group_name` SET status = 'p' WHERE filename =" + "'" + file_name + "'"
        mycursor.execute(update_status)
        mydb.commit()
        
        with open(filepath) as fp:

            status_status = "SELECT filename FROM `group_name` WHERE filename =" + "'" + file_name + "'"
            mycursor.execute(status_status)
            status_result = mycursor.fetchall()

            for row in status_result:

                for line in fp:
                    print("Active file local file: " + file_name + ", Active file from DB: " +  row[0])
                    new_filename_and_path = '/home/ubuntu/file-retriever/output/' + 'output_' + line
                    new_filename_and_path = new_filename_and_path.strip()
                    outF = open(new_filename_and_path, "w")
                    try:

                        # URL of the directory with 2M files text files

                        url = "https://domain.com/data/crime_data/split/" + line
                        print('URL: ' + url)
                        req=Request(url)
                        req.add_header('Authorization', 'Basic %s' % encoded_credentials.decode("ascii")) 
                        contents = urlopen(req).read() 
                        contents = str(contents)
                        print("Contents: " + contents)
                        print('Filename: ' + line)

                        for item in contents.split("\n"):

                            # replace "b" with the string you want to sort
                            # each text file that contains "b" will be saved in the output folder /home/ubuntu/file-retriever/output/

                            if "b" in item:
                                #item = item.rstrip("\n")
                                print("Item: " + item)
                                outF.write(item)
                                #outF.write("\n")
                                print("File written: " + new_filename_and_path)
                                print("###")
                            else:
                                print("No match.")
                    except Exception as err:
                        print(str(err))
                        print('')

            # update the status of the group file to "d" for "done"

            update_status = "UPDATE `group_name` SET status = 'd'"
            mycursor.execute(update_status)
            mydb.commit()
            outF.close() 
            mydb.close() 