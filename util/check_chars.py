import psycopg2
AFOS = psycopg2.connect(database='afos', host='iemdb', port=5555,
                        user='nobody')
acursor = AFOS.cursor()


acursor.execute("""SELECT data, entered at time zone 'UTC'
    from products_2016_0106 WHERE pil = 'AFDOTX' and
    entered = '2016-04-10 23:13+00' ORDER by entered""")
for row in acursor:
    res = [i if ord(i) > 126 else '' for i in row[0]]
    res2 = ','.join([r for r in res if r != ''])
    if res2 != '':
        print row[1].strftime("%Y%m%d-%H%M"), res2, row[0]
