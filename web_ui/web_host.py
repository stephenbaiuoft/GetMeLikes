from flask import render_template, redirect, url_for, request, g
from web_ui import webapp
from cassandra.cluster import Cluster

@webapp.route('/',methods=['GET'])
@webapp.route('/index',methods=['GET'])
@webapp.route('/main',methods=['GET'])
# Display an HTML page with links

def main():
    print("entering main function")
    return render_template("main.html",title="Landing Page")

@webapp.route('/random_id',methods=['GET'])
# Display random key word count for some id
def show_random_id():
    print("Entered Show Random ID")

    sql_cmd = "select id, count_id from t2 limit 10;"
    # test connection
    cluster = Cluster(['ec2-52-40-21-59.us-west-2.compute.amazonaws.com',
                       'ec2-34-216-135-39.us-west-2.compute.amazonaws.com'])
    session = cluster.connect('twitter')

    rows = session.execute(sql_cmd)
    display_rez = []

    sql_cmd2= 'select * from t2 where count_id > 1 limit 10 Allow Filtering'
    rows2 = session.execute(sql_cmd2)

    for user_row in rows:
        display_rez.append( (user_row[0], user_row[1]))
        print (user_row[0],"|" ,user_row[1])

    for user_row in rows2:
        display_rez.append( (user_row[0], user_row[1]))


    return render_template("user_result.html", items=display_rez)
