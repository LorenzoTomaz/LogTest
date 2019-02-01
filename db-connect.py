#!/usr/bin/env python3


import psycopg2

code = open('stream.txt', 'w')


def consulta_views_slug():
    db = psycopg2.connect("dbname=news")
    con = db.cursor()
    con.execute("select  B.title as title, count(A.path) as views \
from log as A, articles as B where  concat('/article/', B.slug) = \
A.path  and status = '200 OK' group by B.title order by count(A.path)\
 DESC limit 3;")
    return con.fetchall() 
a = consulta_views_slug()

for x,i in enumerate(a):
    print('* "{}" - {} views'.format(a[x][0], a[x][1]))
    code.write('\n* "{}" - {} views \n'.format(a[x][0], a[x][1]))

def consulta_views_autor():
    db = psycopg2.connect("dbname=news")
    con = db.cursor()
    con.execute("select d.nome as author, d.soma as views from(select \
a.sum as soma, c.name as nome, a.author from (select b.author, sum(b.ct)\
as sum from (select A.author as author,  A.slug as slug, B.path as path,\
count(B.path) as ct  from articles as A, log as B, authors as C  where \
concat('/article/', A.slug) = B.path and A.author = C.id and B.status =\
 '200 OK' group by slug, B.path, A.author order  by ct) as b group by \
b.author) as a, authors as c where a.author = c.id order by a.sum DESC)\
 as d;")
    return con.fetchall()
a = consulta_views_autor()

for x,i in enumerate(a):
    print('* {} - {} views'.format(a[x][0], int(a[x][1])))
    code.write('\n* {} - {} views \n'.format(a[x][0], int(a[x][1])))


def consulta_percent():
    db = psycopg2.connect("dbname=news")
    con = db.cursor()
    con.execute("select to_char(B.time, 'DD/MM/YYYY')as  time, B.porcentagem\
 as porcentagem from (select DATE(time)  as time, count(status)  filter\
 (where status = '200 OK') as ok, count(status) filter (where  status \
!= '200 OK') as error, cast(count(status) filter (where status != '200 OK'\
) as decimal)/cast(count(status) filter  (where status = \
 '200 OK') as decimal) as porcentagem from log group by \
  DATE(time)) as B where B.porcentagem > 0.0100;")
    return con.fetchall()
a = consulta_percent()

for x,i in enumerate(a):
    print('* "{}" - {} % errors'.format(a[x][0], round(100.00*float(a[x][1]),2)))
    code.write('\n* {} - {}% errors \n'.format(a[x][0], \
round(100.00*float(a[x][1]),2)))
