from flask import render_template, redirect, url_for, request, g
from web_ui import webapp
from cassandra.cluster import Cluster
from flask import Markup

# helper methods
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        cluster = Cluster(['ec2-52-40-21-59.us-west-2.compute.amazonaws.com',
                           'ec2-34-216-135-39.us-west-2.compute.amazonaws.com'])
        session = cluster.connect('twitter')
        db = g._database = session
    return db


@webapp.route('/',methods=['GET'])
@webapp.route('/index',methods=['GET'])
# Display an HTML page with links
@webapp.route('/main', methods=['GET','POST'])
def main():
    # init database already
    get_db()
    print("entering main function")
    return render_template("main.html", title="Landing Page")



@webapp.route('/bartest')
def show_bar_test():
    # now make sure it's a list of lists!!!!!
    word_labels = [ ["My", "long", "long", "long", "label"] ,
                    ['realDonaldTrump Merylsayshi'],
                    ['Tonight'],
                    ]
    count_values = [250864,
                   189237,
                   119746]
    print(count_values)
    print(word_labels)
    # do donald trump for now
    return render_template('bar.html', user_name='Testing',
                           type_name="retweets",
                           values=[250864,189237,119746], labels=word_labels)


# show top_word_list, given an id
@webapp.route('/top_word_report', methods=['GET','POST'])
# Display random key word count for some id
def top_word_list():
    # add _word as an array form to _list
    # if the _word is too long
    def add_to_display_list(_list, _word):
        if len(_word) <=20:
            _list.append([_word])
            return

        l = len(_word)
        _s = 0
        _e = 1
        element = []
        while _s < l/20:
            if _s == 0:
                element.append(_word[_s*20: _e*20])
            else:
                element.append('-'+_word[_s * 20: _e * 20])
            _s = _e
            _e += 1
        _list.append(element)

    user_name = request.form['user_name']
    date = request.form['date']
    count_values = []
    word_labels = []
    # do donald trump for now
    db_session = get_db()

    top_limit = 5
    sql_cmd = "select rt_entity_list from demo_top_list where user_name =" \
              " \'" + user_name + "\' ALLOW FILTERING;"


    try:
        rows = db_session.execute(sql_cmd)
        for row in rows:
            for _tuple in row[0]:
                print("tuple_value is: ", _tuple)
                if top_limit == 0:
                    break
                top_limit -= 1
                count_values.append(_tuple[1])
                add_to_display_list(word_labels, _tuple[0])

        # debugging purpose
        print(word_labels)
        print(count_values)


        sql_cmd_bytime = "select creation_date, rt_entity_list from demo_top_month_list" \
                         + " where user_name =" \
                           " \'" + user_name + "\' and creation_date > \'" + date + "\' ;"
        print(sql_cmd_bytime)
        # time series query
        rows = db_session.execute(sql_cmd_bytime)

        # tuple of (create-date, top_word, retweet_count)
        table_data = []
        for row in rows:
            day = str(row[0])[:-2]
            if len(row[1]) > 1:
                w = row[1][0][0]
                c = row[1][0][1]
                # insert @ beginning
                table_data.insert(0, (str(row[0])[:-3],
                               row[1][0][0], row[1][0][1]) )

        print(table_data)

        return render_template('bar_display.html', user_name=user_name,
                               type_name="retweets",
                               values=count_values,
                               labels=word_labels,
                               table_data=table_data
                               )

    except Exception as e:
        print("An Error Occur: default values")
        # or pop a window?

        word_labels = [['Hey realDonaldTrump'], ['realDonaldTrump Merylsayshi'],
                       ['Tonight']]
        count_values = [250864, 189237, 119746]
        table_data  = (("n/a", "n/a", "n/"))
        return render_template('bar_display.html', user_name='Query Went Wrong',
                               type_name="retweets",
                               values=count_values, labels=word_labels,
                               table_data = table_data
                               )




@webapp.route('/random_id', methods=['GET'])
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
