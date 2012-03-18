"""
  Need an app to save authorizations for iembot from twitter...
 $Id$:
"""
import sys
sys.path.insert(0,'../')
import secret
from oauthtwitter import OAuthApi
import pprint
import iemdb
MESOSITE = iemdb.connect('mesosite', bypass=True)
mcursor = MESOSITE.cursor()

twitter = OAuthApi(secret.consumer_key, secret.consumer_secret)

# Get the temporary credentials for our next few calls
#temp_credentials = twitter.getRequestToken()

# User pastes this into their browser to bring back a pin number
#print(twitter.getAuthorizationURL(temp_credentials))

# Get the pin # from the user and get our permanent credentials
#oauth_verifier = raw_input('What is the PIN? ')
#access_token = twitter.getAccessToken(temp_credentials, oauth_verifier)



#print("oauth_token: " + access_token['oauth_token'])
#print("oauth_token_secret: " + access_token['oauth_token_secret'])

#botuser = raw_input("What is username?")
#mcursor.execute("DELETE from oauth_tokens where username = '%s'" % (botuser,))
#mcursor.execute("INSERT into oauth_tokens values ('%s','%s','%s')" % (botuser, access_token['oauth_token'], access_token['oauth_token_secret']) )

#mcursor.close()
#MESOSITE.commit()

# Do a test API call using our new credentials
twitter = OAuthApi(secret.consumer_key, secret.consumer_secret, 
  access_token['oauth_token'], access_token['oauth_token_secret'])
print dir(twitter)
#twitter.UpdateStatus("test test test")
print twitter.ApiCall("account/verify_credentials")
user_timeline = twitter.GetUserTimeline()

pp = pprint.PrettyPrinter(indent=4)
pp.pprint(user_timeline)
