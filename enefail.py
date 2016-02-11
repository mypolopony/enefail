#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: mypolopony
# @Date:   2016-01-25 22:19:51
# @Last Modified 2016-02-02
# @Last Modified time: 2016-02-02 02:41:21

import credentials
import collections
import logging
import email.utils
import string
import time
import datetime
import glob
import re
import MySQLdb as sql
from warnings import filterwarnings

# Setup, i.e. wayward home for global variables (keep them safe!)
logging.basicConfig(filename='enefail.log', level=logging.INFO)
dbname = 'enefail'
datadir = 'enron_with_categories'
connection = sql.connect(passwd=credentials.db_password, db=dbname)
dbcursor = connection.cursor()
filterwarnings('ignore', category=sql.Warning)


class Message:

    '''
    Basic classes, because I'm a sucker for them, and I prefer them to
    unnamted tuples or even dicts especially for core types that will
    be central to the business. It's not really all that important for now
    '''

    def __init__(self, folder, id):
        self.folder = folder
        self.id = id

    def extractbody(self, lines):
        '''
        Here, as below, we are assuming the the headers are unbroken and that
        the body of the email follows afterwards
        '''
        self.body = lines[lines.index('\n') + 1:]

    def extractheader(self, text):
        '''
        We're presuming here that the headers are unbroken. This is a 
        naive assumption for now, but makes theoretical sense. We also notice
        that some emails do not have the essential fields, like To or From.

        What shall we do with these? Here, I've safely created the dictionary
        so that it will have non-null values, but in the future, we probably
        want to flag these as potentially corrupted
        '''
        self.headers = dict.fromkeys(['From', 'To', 'Subject'], '')
        for line in text:
            if line is not '\n':
                try:
                    linesplit = line.split(':', 1)
                    self.headers[linesplit[0]] = linesplit[
                        1].strip().replace('\n', '')
                    current = linesplit[0]
                # This is a fun case where there are newlines in the headers!
                except:
                    self.headers[current] += line.replace('\n', '')
            else:
                break
        self.headers['To'] = self.headers['To'].lower()
        self.headers['From'] = self.headers['From'].lower()

        # Some basic editing of the determined types
        self.headers['Date'] = time.strftime(
            '%Y-%m-%d %H:%M:%S', email.utils.parsedate(self.headers['Date']))
        emailpattern = re.compile('[.\w]+@[^ ,]+')
        recipients = emailpattern.findall(self.headers['To'])
        self.headers['To'] = recipients
        senders = emailpattern.findall(self.headers['From'])
        self.headers['From'] = senders

    def extractcats(self, text):
        '''
        Categories will be a list of tuples here, for (n1, n2, freq)
        '''

        self.categories = list()
        for line in text:
            if line is not '\n':
                linesplit = line.split(',')
                self.categories.append(
                    (linesplit[0], linesplit[1], linesplit[2].replace('\n', '')))


def readcategories():
    '''
    Category digest and SQL insertion. This can be done more easily than
    the messages, so we'll just set them up quickly.
    '''

    # Create the database, if it doesn't exist!
    try:
        logging.info('========\nCreating CATEGORY Table')
        cmd =  'CREATE TABLE `categories` (' \
            'id int unsigned auto_increment not null,' \
            '`n1` INT,' \
            '`n2` INT,' \
            '`catname` VARCHAR(128),' \
            'primary key (id))'
        dbcursor.execute(cmd)
    except Exception as err:
        logging.warning('Warning!')
        logging.warning(err)

    # look for lines in the format #.##... and insert them
    logging.info('Inserting categories\n')
    searchexp = re.compile('^\d\.\d+')
    with open('{dir}/categories.txt'.format(dir=datadir)) as catfile:
        for line in catfile:
            if searchexp.search(line):  # Found one!
                line = str.split(line, ' ', 1)  # Split on first space
                n1 = line[0].split('.')[0]
                n2 = line[0].split('.')[1]
                catname = line[1].replace('\n', '')

                try:
                    cmd = "INSERT INTO categories (n1,n2,catname) "\
                        "VALUES({n1}, {n2}, '{cat}')".format(
                            n1=n1, n2=n2, cat=catname)
                    dbcursor.execute(cmd)
                except Exception as err:
                    logging.warning('Warning!')
                    logging.warning(err)

    # It's nice to commit at the end, as this is time consuming for large
    # datasets
    connection.commit()


def gathermessages():
    '''
    We want to get the relevant .cats and .txt files, then make sure
    what we have makes sense. If it doesn't we'll do some logging and
    ignore those cases, if they exist
    '''

    logging.info('========\nGathering files. . .')

    # This conveniently removes the pesky categories.txt
    filelist = glob.glob('{dir}/*/**'.format(dir=datadir))
    messages = dict()

    # Gather *potential* messages
    for f in filelist:
        sections = f.split('/')
        folder = sections[1]
        id = sections[2].split('.')[0]

        if id not in messages:
            messages[id] = Message(folder, id)

    orignum = len(messages)
    logging.info('{num} potential messages found\n'.format(num=orignum))

    # Sanity check! # This is necessary to catch .cats without .txts and vice
    # versa
    for id, value in list(messages.items()):
        assoc = glob.glob(
            '{dir}/{folder}/{id}.*'.format(dir=datadir, folder=messages[id].folder, id=id))
        if len(assoc) != 2:  # Something's gone wrong!
            logging.warning('\tNote: id \'{id}'' in folder {folder} has the wrong number of files'.format(
                id=id, folder=messages[id]))
            logging.warning('\tFiles: {flist}\n'.format(flist=assoc))
            messages.pop(id)

    newnum = len(messages)
    pct = '%.2f' % (newnum / orignum * 100)
    logging.info(
        '{num} messages found to be in tact ({pct}%)!'.format(num=newnum, pct=pct))

    return messages


def readmessages(messagelist):
    '''
    Here, we'll take in a group of newly born Message items and read the files
    to bulk up the Message object
    '''
    logging.info('========\nReading Messages. . .')
    for message in messagelist.keys():
        msg = messagelist[message]
        with open('{d}/{f}/{id}.txt'.format(d=datadir, f=msg.folder, id=msg.id)) as messagefile:
            lines = messagefile.readlines()
            msg.extractheader(lines)
            msg.extractbody(lines)
        with open('{d}/{f}/{id}.cats'.format(d=datadir, f=msg.folder, id=msg.id)) as catsfile:
            lines = catsfile.readlines()
            msg.extractcats(lines)

    return messagelist


def clean_string(text):
    '''
    SQL can be awful annoying when it comes to escaping messy strings. There is
    a version of clean_string that comes with MySQLdb but it's not so transparent.
    '''
    whitelist = string.ascii_letters + string.digits + ' ' + '/' + '?' + '\\' + '\t' + '.' + '!' + '@' + '#' + '$' + '%' + \
        '&' + '*' + '(' + ')' + '_' + '-' + '=' + '+' + ':' + ';' + \
        '|' + '[' + ']' + '{' + '}' + '<' + '>' + '~' + '^' + '`'
    clean_string = ''
    for character in text:
        if character in whitelist:
            clean_string += character

    return clean_string.replace("'", "\'")


def sendmessages(messages):
    '''
    Here, we send the messages to the database. Of course, we must create
    the table first if it does not yet exist! We'll just do a few of the key
    elements, for illustration
    '''
    try:
        logging.info('========\nCreating MESSAGES Table')
        cmd =  'CREATE TABLE `messages` (' \
            '`messageid` VARCHAR(128) UNIQUE,' \
            '`date` DATETIME,' \
            '`subject` VARCHAR(1024),' \
            '`body` MEDIUMTEXT,' \
            'primary key (messageid))'
        dbcursor.execute(cmd)
    except Exception as err:
        logging.warning('Warning!')
        logging.warning(err)

    try:
        logging.info('========\nCreating MESSAGE_PATH Table')
        cmd =  'CREATE TABLE `message_path` (' \
            'id int unsigned auto_increment not null,' \
            '`messageid` VARCHAR(128) REFERENCES messages(messageid),' \
            '`sender` int REFERENCES users(id),' \
            '`recipient` int REFERENCES users(id),' \
            'primary key (id),' \
            'UNIQUE KEY `path` (`messageid`,`sender`,`recipient`))'
        dbcursor.execute(cmd)
    except Exception as err:
        logging.warning('Warning!')
        logging.warning(err)

    try:
        logging.info('========\nCreating MESSAGE_CATEGORY Table')
        cmd =  'CREATE TABLE `message_category` (' \
            '`messageid` VARCHAR(128) REFERENCES messages(messageid),' \
            '`n1` INT,' \
            '`n2` INT,' \
            '`freq` INT,' \
            'primary key (messageid))'

        dbcursor.execute(cmd)
    except Exception as err:
        logging.warning('Warning!')
        logging.warning(err)

    try:
        logging.info('========\nCreating USERS Table')
        cmd =  'CREATE TABLE `users` (' \
            'id int unsigned auto_increment not null,' \
            '`email` VARCHAR(128) UNIQUE,' \
            'primary key(id))'

        dbcursor.execute(cmd)
    except Exception as err:
        logging.warning('Warning!')
        logging.warning(err)

    connection.commit()

    logging.info('========\nSending messages to DB. . .')
    for message in messages.keys():
        try:
            msg = messages[message]
            messageid = msg.headers['Message-ID']
            date = msg.headers['Date']
            senders = msg.headers['From']
            recipients = msg.headers['To']

            for user in recipients + senders:
                selectcmd = "SELECT * FROM users WHERE email = '{u}'".format(
                    u=user)
                dbcursor.execute(selectcmd)
                id = dbcursor.fetchone()
                if not id:
                    insertcmd = "INSERT IGNORE INTO users (email) VALUES('{u}')".format(
                        u=user)
                    dbcursor.execute(insertcmd)
                    id = dbcursor.fetchone()

            # First, let's insert the message
            cmd = "INSERT IGNORE INTO messages(messageid,date,subject,body) " \
                "VALUES('{m}',STR_TO_DATE('{d}','%Y-%m-%d %T'),'{s}','{b}')".format(
                    m=messageid,
                    d=date,
                    s=clean_string(msg.headers['Subject']),
                    b=clean_string(''.join(msg.body)))

            dbcursor.execute(cmd)
            connection.commit()

            # Now we can do the message_paths
            # We'll assume for now that there is one sender per email
            cmd = "SELECT * FROM users WHERE email = '{u}'".format(
                u=senders[0])
            dbcursor.execute(cmd)
            sendid = dbcursor.fetchone()[0]
            for to in recipients:
                cmd = "SELECT * FROM users WHERE email = '{u}'".format(u=to)
                dbcursor.execute(cmd)
                recid = dbcursor.fetchone()[0]
                cmd = "INSERT IGNORE INTO message_path(messageid,recipient,sender) " \
                    "VALUES('{mid}',{to},{sender})".format(
                        mid=messageid, to=recid, sender=sendid)
                dbcursor.execute(cmd)

            # And finally the message categories
            for cat in msg.categories:
                cmd = "INSERT IGNORE INTO message_category(messageid,n1,n2,freq) " \
                    "VALUES('{mid}',{n1},{n2},{freq})".format(
                        mid=messageid, n1=cat[0], n2=cat[1], freq=cat[2])
                dbcursor.execute(cmd)

            connection.commit()

        except Exception as err:
            logging.info('Uh oh! Message {id} seems to be a bit broken. . . )'.format(
                id=msg.id))
            logging.info(err)


def analyze():
    '''
    Here we're just going to do some relatively simple queries to investigate
    the data we've put in the database.
    '''

    # 1: How many emails did each person receive each day?
    # We can easily do this across all users but let's just pick one
    # Let's just say we know a guy at jeff.donahue@enron.com

    guy = 'jeff.donahue@enron.com'
    dbcursor.execute(
        "SELECT id FROM users WHERE email = '{user}'".format(user=guy))
    guyid = dbcursor.fetchone()[0]

    dbcursor.execute(
        "SELECT m1.messageid, m2.date FROM message_path m1 INNER JOIN messages m2 ON m1.recipient = {id}".format(id=guyid))
    guymails = dbcursor.fetchall()

    days = dict()
    for mail in guymails:
        stringdate = mail[1].strftime('%Y-%m-%d')
        if stringdate in days.keys():
            days[stringdate] += 1
        else:
            days[stringdate] = 1
    logging.info(
        '========\nPrinting Daily Message Count for {user}'.format(user=guy))

    odays = collections.OrderedDict(sorted(days.items()))
    for day, num in odays.items():
        logging.info('{d}: {n}'.format(d=day, n=num))

    # 2a: Who has sent the largest number of direct emails?
    logging.info('========\nCalculating top five direct message senders. . .')
    dbcursor.execute(
        "SELECT messageid,sender FROM message_path GROUP BY messageid,sender HAVING count(*) = 1")
    senders = dbcursor.fetchall()

    common = collections.Counter([s[1] for s in senders]).most_common()
    for top in range(0, 5):
        dbcursor.execute(
            "SELECT email FROM users WHERE id = '{user}'".format(user=common[top][0]))
        email = dbcursor.fetchone()[0]
        logging.info('#{rank} {uid}: {num}'.format(
            rank=top + 1, uid=email, num=common[top][1]))

    # 2b: Who has received the largest number of direct emails?
    # (there is most likely a fancier query that could be constructed, but this works!)
    logging.info('========\nCalculating top five broadcast recipients. . .')
    dbcursor.execute(
        "SELECT messageid, count(*) FROM message_path GROUP BY messageid HAVING count(*) > 1")
    broadcasts = dbcursor.fetchall()

    broadrecs = dict()
    for message in broadcasts:
        dbcursor.execute(
            "SELECT recipient FROM message_path WHERE messageid='{mid}'".format(mid=message[0]))
        recipients = dbcursor.fetchall()
        for r in recipients:
            if r[0] in broadrecs.keys():
                broadrecs[r[0]] += 1
            else:
                broadrecs[r[0]] = 1

    obroadrecs = collections.OrderedDict(
        sorted(broadrecs.items(), key=lambda t: t[1]))
    for top in range(0, 5):
        record = obroadrecs.popitem()
        dbcursor.execute(
            "SELECT email FROM users WHERE id = '{user}'".format(user=record[0]))
        email = dbcursor.fetchone()[0]
        logging.info(
            '#{rank} {uid}: {num}'.format(rank=top, uid=email, num=record[1]))

    # 3: What are the five fastest response times?
    # First, let's find out if there are any reciprocal connections.
    # Sets are naturally unordered, and oddly enough, a string representation can be
    # used as a hash!
    #
    # (as it turns out, with the hash we gain from losing directionality, which
    # would be helpful if there were not duplicates, which there are. . .)
    logging.info('========\nFinding some really fast responders. . .')

    # For reference --
    # Pairs will be:
    # pairs[hash_string] = {<message_id>: {subject_set}, <message_id>: {subject_set}}
    # Matches will be:
    # matches = [(userpair, <message_id>, <message_id>, time1, time2,
    # timediff)]

    # Here, we grab correspondances between all sets of two parties
    dbcursor.execute("SELECT messageid, recipient, sender FROM message_path")
    pairs = dict()
    for triplet in dbcursor:			# Thanks MySQLdb for this little iterator!
        hash = set((triplet[1], triplet[2])).__str__()
        if hash not in pairs.keys():
            pairs[hash] = dict()
        pairs[hash][triplet[0]] = set()

    logging.info(
        '{num} unique correspondance pairs found!'.format(num=len(pairs.keys())))

    # Now, we want to see if the messages we found between two parties have similar
    # subject lines
    matches = list()
    for p in pairs.keys():
        if len(pairs[p]) > 1:			# No sense looking otherwise
            for message in pairs[p].keys():
                dbcursor.execute(
                    "SELECT subject FROM messages WHERE messageid='{mid}'".format(mid=message))
                pairs[p][message] = set(
                    dbcursor.fetchone()[0].lower().split(' '))
                for message2 in pairs[p]:
                    if message2 is not message:
                        if len(pairs[p][message].symmetric_difference(pairs[p][message2])) <= 1:
                            matches.append((p, message, message2))

    # There are some! Neat! Let's see how far apart they were in time
    for idx, m in enumerate(matches):
        dbcursor.execute(
            "SELECT date FROM messages WHERE messageid='{mid}'".format(mid=m[1]))
        time1 = dbcursor.fetchone()[0]
        dbcursor.execute(
            "SELECT date FROM messages WHERE messageid='{mid}'".format(mid=m[2]))
        time2 = dbcursor.fetchone()[0]
        diff = abs(time2 - time1).total_seconds()
        if diff == 0:						# This is not such a bad semi-hack for the
            diff = float('Inf')				# duplicates problem. . .
        timem = m + (time1, time2, diff,)
        matches[idx] = timem

    # Now, we sort on the correct element
    matches = sorted(matches, key=lambda x: x[5])

    # And print the top five. Make sure to sanity check by checking the parties and
    # the subject line! For now, let's just forget about the fact that because the
    # relationships are reciprocal, the list is doubled and we have to skip :)
    # :)
    for m in range(0, 9, 2):
        logging.info('****')
        match = matches[m]
        dbcursor.execute(
            "SELECT subject FROM messages WHERE messageid='{mid}'".format(mid=match[1]))
        subject1 = dbcursor.fetchone()[0]
        dbcursor.execute(
            "SELECT subject FROM messages WHERE messageid='{mid}'".format(mid=match[2]))
        subject2 = dbcursor.fetchone()[0]

        logging.info('Correspondants: {users}'.format(users=match[0]))
        logging.info('Subjects: {subs}'.format(subs=(subject1, subject2)))
        logging.info('Messages: {msgs}'.format(msgs=(match[1], match[2])))
        logging.info('Times: {times}'.format(times=(match[3], match[4])))
        logging.info('Total time (sec): {time}'.format(time=float(match[5])))


def main():
    logging.info('========\nCStarting up!')

    readcategories()
    messagelist = gathermessages()
    messages = readmessages(messagelist)
    sendmessages(messages)
    analyze()

if __name__ == "__main__":
    main()
