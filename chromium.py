from tkFileDialog import *
from Tkinter import *
from tkMessageBox import *
import mysql.connector
import json

TABLES = {}
TABLES['t_change'] = (
    'create table t_change('
    'id int(11) primary key not null auto_increment,'
    'description text default null,'
    'cc text default null,'
    'reviewers text default null,'
    'owner_email varchar(100) default null,'
    'private varchar(20) default null,'
    'base_url text default null,'
    'owner varchar(50) default null,'
    'subject text default null,'
    'created varchar(45) default null,'
    'patchsets text default null,'
    'modified varchar(45) default null,'
    'closed varchar(10) default null,'
    'commit varchar(10) default null,'
    'issue int(11) default null'
    ') engine=innodb default charset=utf8mb4'
    )

TABLES['t_messages'] = (
    'create table t_messages('
    'id int(11) primary key not null auto_increment,'
    'sender varchar(90) default null,'
    'recipients text default null,'
    'text longtext default null,'
    'disapproval varchar(10) default null,'
    'date varchar(45) default null,'
    'approval varchar(10) default null,'
    'changeId int(11) default null'
    ') engine=innodb default charset=utf8mb4 collate utf8mb4_bin'
    )

# db
db = None
# cursor
cursor = None

main = Tk()
main.title('Import Chromium')

# sql configuration
sqlconfig = LabelFrame(main, text='Database Configuration')
sqlconfig.grid(column=0, row=0)

hostlabel = Label(sqlconfig, text='Host: ').grid(column=0, row=0, sticky=E)
hostvar = StringVar()
hostvaluelabel = Entry(sqlconfig, textvariable=hostvar, width='10')
hostvaluelabel.grid(column=1, row=0)
hostvar.set('127.0.0.1')

userlabel = Label(sqlconfig, text='User Name: ').grid(column=0, row=1, sticky=E)
uservaluelabel = Entry(sqlconfig, width='10')
uservaluelabel.grid(column=1, row=1)

pwdlabel = Label(sqlconfig, text='Password: ').grid(column=0, row=2, sticky=E)
pwdvaluelabel = Entry(sqlconfig, width = '10', show='*')
pwdvaluelabel.grid(column=1, row=2)

# data file
datadir = LabelFrame(main, text='Data Directory')
datadir.grid(column=0, row=1)

dirlabel = Label(datadir, text='Data Direcotry: ').grid(column=0, row=0)
dirvar = StringVar()
dirvaluelabel = Entry(datadir, textvariable=dirvar, width='50', state=DISABLED).grid(column=1, row=0)
datafile = None
def chooseFile():
    global datafile
    datafile = askopenfilename()
    dirvar.set(datafile)
fileButton = Button(datadir, text='Choose File', command=chooseFile)
fileButton.grid(column=2, row=0)

# button action: Import
def importData():
    global db
    global cursor
    
    host = hostvaluelabel.get()
    user = uservaluelabel.get()
    pwd = pwdvaluelabel.get()
    if host == '':
        showwarning('Empty host', 'Please input host.')
        return
    elif user == '':
        showwarning('Empty user name', 'Please input user name.')
        return
    elif datafile == None:
        showwarning('Empty data file', 'Please choose data file.')
    
    config = {
        'host': host,
        'user': user,
        'password': pwd,
        'charset': 'utf8mb4'
        }
    db = None
    try:
        db = mysql.connector.connect(**config)
    except:
        showerror('Access denied', 'Please check database configuration.')
        return

    cursor = db.cursor()
    cursor.execute('drop database if exists chromium')
    cursor.execute('create database chromium default charset utf8mb4 collate=utf8mb4_unicode_ci')
    db.cmd_init_db('chromium')

    for name, sql in TABLES.iteritems():
        cursor.execute(sql)

    # read data
    jsonfile = open(datafile)
    while True:
        line = jsonfile.readline().strip()
        
        if not line:
            break
        changeobj = json.loads(line)
        saveChange(changeobj)
        
    showinfo('Done', 'Import to database completed!')

# save change
def saveChange(changeobj):
    global cursor
    keys = changeobj.keys()
    sql = 'insert into t_change('
    for i in range(len(keys)):
        if not keys[i] == 'messages':
            if i < len(keys) - 1:
                sql = sql + keys[i] + ', '
            else:
                sql = sql + keys[i] + ')'
    sql = sql + ' values('
    param = ()
    for i in range(len(keys)):
        if not keys[i] == 'messages':
            value = changeobj[keys[i]]

            if type(value) is list:
                if len(value) > 0:
                    if type(value[0]) is int:
                        value = map(str, value)
                    value = ','.join(value)
                else:
                    value = ''

            param = param + (value, )
            
            if i < len(keys) - 1:
                sql = sql + '%s, '
            else:
                sql = sql + '%s)'

    cursor.execute(sql, param)
    changeId = cursor.lastrowid
    saveMessages(changeobj['messages'], changeId)

# save messages
def saveMessages(messages, changeId):
    global db
    global cursor
    
    for messageobj in messages:
        messageobj['changeId']= changeId

        sql = 'insert into t_messages('
        keys = messageobj.keys()
        for i in range(len(keys)):
            if i < len(keys) - 1:
                sql = sql + keys[i] + ', '
            else:
                sql = sql + keys[i] + ')'
        sql = sql + ' values('
        param = ()
        for i in range(len(keys)):
            value = messageobj[keys[i]]

            if type(value) is list:
                if len(value) > 0:
                    if type(value[0]) is int:
                        value = map(str, value)
                    value = ','.join(value)
                else:
                    value = ''

            param = param + (value, )
            
            if i < len(keys) - 1:
                sql = sql + '%s, '
            else:
                sql = sql + '%s)'

        cursor.execute(sql, param)

    db.commit()
            
importButton = Button(main, text='Import', command=importData).grid(column=0, row=2)

main.mainloop()
