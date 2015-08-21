"""
  Add channels as we segregate out a product form the main XXX channel
"""
import psycopg2
import sys

AWIPSID = sys.argv[1]

pgconn = psycopg2.connect(database='mesosite', host='iemdb')
cursor = pgconn.cursor()

# figure out our base XXX channels
cursor.execute("""SELECT id from iembot_channels where length(id) = 3""")
add_channels = []
for row in cursor:
    add_channels.append("%s%s" % (AWIPSID, row[0]))

# Create channels as necessary
channel_adds = 0
for channel in add_channels:
    cursor.execute("""SELECT * from iembot_channels where id = %s""", (channel,
                                                                       ))
    if cursor.rowcount == 0:
        cursor.execute("""INSERT into iembot_channels(id, name)
        VALUES (%s,%s)""", (channel, channel))
        channel_adds += 1

# Update room subscriptions
room_adds = 0
for channel in add_channels:
    cursor.execute("""SELECT roomname from iembot_room_subscriptions
    WHERE channel = %s""", (channel[3:],))
    rooms = []
    for row in cursor:
        rooms.append(row[0])
    for rm in rooms:
        cursor.execute("""INSERT into iembot_room_subscriptions
        (roomname, channel) VALUES (%s,%s)""", (rm, channel))
        room_adds += 1

# Update twitter
twitter_adds = 0
for channel in add_channels:
    cursor.execute("""SELECT screen_name from iembot_twitter_subs
    WHERE channel = %s""", (channel[3:],))
    pages = []
    for row in cursor:
        pages.append(row[0])
    for page in pages:
        cursor.execute("""INSERT into iembot_twitter_subs
        (screen_name, channel) VALUES (%s,%s)""", (page, channel))
        twitter_adds += 1

print(('%s channel_adds %s  room_adds %s twitter_adds %s'
       ) % (AWIPSID, channel_adds, room_adds, twitter_adds))
cursor.close()
pgconn.commit()
