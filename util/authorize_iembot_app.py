import oauth2 as oauth
from oauthtwitter import OAuthApi
import pprint
import pg
mydb = pg.connect('mesosite')

consumer_key = ""
consumer_secret = ""

twitter = OAuthApi(consumer_key, consumer_secret)

# Get the temporary credentials for our next few calls
temp_credentials = twitter.getRequestToken()

# User pastes this into their browser to bring back a pin number
print(twitter.getAuthorizationURL(temp_credentials))

# Get the pin # from the user and get our permanent credentials
oauth_verifier = raw_input('What is the PIN? ')
access_token = twitter.getAccessToken(temp_credentials, oauth_verifier)

print("oauth_token: " + access_token['oauth_token'])
print("oauth_token_secret: " + access_token['oauth_token_secret'])

botuser = raw_input("What is username?")
mydb.query("INSERT into oauth_tokens values ('%s','%s','%s')" % (botuser, access_token['oauth_token'], access_token['oauth_token_secret']) )

# Do a test API call using our new credentials
#twitter = OAuthApi(consumer_key, consumer_secret, access_token['oauth_token'], access_token['oauth_token_secret'])
#user_timeline = twitter.GetUserTimeline()

#pp = pprint.PrettyPrinter(indent=4)
#pp.pprint(user_timeline)
