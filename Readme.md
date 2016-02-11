# enefail
<b>author:</b> selwyn-lloyd mcpherson (<a href='mailto:selwyn.mcpherson@gmail.com'>selwynlloyd.mcpherson@gmail.com</a>)  
<b>python version:</b> 3.5.0 ('cause it's new and fancy!)

## Aim
enefail (aka enron fail) is a basic exhibition to demonstrate how to ingest and decipher semi-structured, interconnected data. Ideally, we would like to ask some basic questions, and check out the results, as well as explore the dataset and how we might tackle larger datasets of a similar size. 

# Setup
It's fun to use virtual environments!
```
tar xvf enefail.tar.gz
cd enefail
virtualenv enefail
source enefail/bin/activate
pip install -r requirements.txt

wget http://bailando.sims.berkeley.edu/enron/enron_with_categories.tar.gz
tar xvf enron_with_categories.tar.gz
rm enron_with_categories.tar.gz
```

## Execution
For ease of use, a solution set to the problems provided is output as `answers.txt`, but if you have the chance, do run the scripts to see some unessential info and mechanics and so forth; everything should run fine!

## SQL implementation
There are lots of ways to connect to mysql databases. I really like SQLAlchemy, mostly because of it's nicely object orientated. It can be a weird chimera, though, because it tries to bridge the gap between SQL tables and programatic classes. . . but it works well if designed properly. You don't have to use the OOP bit of course, and it works just as well as a raw connector but it isn't really at all necessary for this example. I would usually use the official connector from dev.mysql.com but it's not quite fit for python=3.5; here, I'll use MySQLdb, which is pretty barebones but can easily do the job. 

I set up my 'important' credentials separately, but you can see the settings I'm using on my local machine.

The tables are pretty straightforward:
**categories:** although not heavily here, is a straightofrward representation of the category description file (_id_, _n1_, _n2_, and _catname_)
**message_category:** conveys the category for each file. We have _n1_, _n2_ and _freq_. In theory, it would be decent of us to not reuse _n1_ and _n2_ and just point to the categories table, but again, since it's not being used here, I thought not to bother
**messages** contains the beefy bits of the email, including the _subject_ and _body_ as well as the _date_. It also includes the all-important _messageid_, which will be referenced throughout
**message_path** is an essential abstraction that separates the _sender_'s and _recipient_'s away from the **messenges** table, since we do care very much about the individual correspondances.
**users** is as it sounds, with an _email_ field.

## Sanity Check 1 -- Data Integrity
First, it will be important to see what kind of data we're working with. We'll need a general idea of how many messages we have and we ant to make sure that the number of `.cats` files equals the number of `.txt` files. This may be trivial in a small sample but it's essential for larger ones.

## Let's ingest!
There are three types of sources that we need to account for. The first is `categories.txt`. We don't want to rely on a priori knowledge, and we want to be able to adapt to future categories. We'll ignore potential syntax errors for now, which would be the job of more thorough error checking. But the solution here is to use general syntax as best as possible.

## Parsing
The actual message can be split into the header and the body. The body will need some work to understand at first. There are several motifs that one might see in an email chain, including different forms of indenting that can give a clue as to the chain's history.

## Sanity Check 2 -- Variance
In my opinion, before commiting anything to a database, we would want to get an idea of what the variance of the data. On eyeball, things can look straightforward, but once each data type is collected, we would expect to check out the values to see how things are looking. For strongly-typed, rigid data, the general form of individual values might be on the order of tens or hundreds, which is manageable.

Really big datasets are also manageable! But with 1000s of 'valid' types, it takes a little finesse to figure out what's invalid, or what's almost valid. That's beyond the scope of this project, though!

## (Theoretical) Sanity Check 3 -- Identity
When dealing with messy data, the attention paid to identity resolution cannot be understated. This becomes more of a problem with natural language processing in which human error is allowed to be misparsed, but thankfully not so much with email headers, which by definition must resolve to a correct address (as long as the file is not corrupted).

As the Carnegie Mellon explanation suggests, there were in fact inconsistencies, particularly in the email address, requiring some thoughtful concessions. I noticed some unorthodox email addresses that may be able to be resolved to a more meaningful entity, so that's always something to consider.

## Reformatting and Committing
For static data, and as much as space will allow (which is generally a lot nowadays), we can do a lot of work locally (or 'cloud-locally') before making permanent changes to a real-life database. This is a little different than with streams, in which there is usually no strong need to hold to and operate on epheremeral data in a serious way. For a small dataset, this is just a point of preference. I don't feel terribly strongly about it really, as I think there are many ways to skin a something-that-is-tasty-and-not-a-cat. We can hold this all in memory and pickle it if we want, or we can use a redis database, which is almost functionally the same, except you don't have to re-load the dataset eerytime you run the script. 

This is a microproject, so I don't feel too badly about holding data in memory.

## Interesting quirks
I like to use classes, so I let a Message be it's own thing that handles some part of the heavy lifting required of it.



## Further Work 1: Message Bodies
This project, thankfully, only requires dealing with headers for the most part. 

Afterwards, the messages themselves are beautifully complex, using different indentings as well as tabs and other assorted white space. Decipehering the trail, and most importantly determing similar messaging despite different formatting, is the juicy stuff -- but takes a bit more analysis.

There are learning algorithms that can smooth over the irregularities, as long as we know what we are looking for -- in this case, trying to hard code every exception would be maddening.

## Further Work 2: iChat?
I happened to run into my own iChat database, which is easily operated on with the same fundamental concepts and, as with group messaging, you have access to messages, chats, contacts, attachments, among others!

## Further Work 3: Graph Databases and Traversal
SQL is wonderful. Everyone knows that. It's familiar, it makes sense, and we all learn it. From my experience, however, networks (like email correspondances) can sometimes not like to be put in relational databases. They find it awful stuffy in there. I can't really argue with forty years of SQL, but in order to better understand just what resources this kind of dataset is begging for, we can think about more natural reprentations that have been optimized for the equipment and infrastructure we have now, as opposed to the infrastructure we had then.

Challenging the status quo is good enough as well as you can back it up and prove that a different solution is easier, more powerful, and faster. In my previous position, I migrated a system from an SQL-based database to a graph database powered by Neo4j. I actually wasn't impressed at first, but it got me thinking, and the kinds of conceptual analyses that my other data scienctist and I could do was really quite beautiful.

The learning curve for graph databses is fascinating, and quixotical, and I couldn't quite work it into this context in a reasonable time without addressing your primary questions, but I tried to work up something that might explain the gestalt.

## Miscellanea
I like to use comments a lot. . . I also am not a pep8 purist, though I appreciate it plenty. Lines can get long sometimes. . . You know how it goes :)

As always, questions and comments are <3!

