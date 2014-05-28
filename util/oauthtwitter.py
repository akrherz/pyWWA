#!/usr/bin/env python
# 
# Copyright under  the latest Apache License 2.0

'''
A modification of the python twitter oauth library by Hameedullah Khan.
Instead of inheritance from the python-twitter library, it currently
exists standalone with an all encompasing ApiCall function. There are
plans to provide wrapper functions around common requests in the future.

Requires:
  simplejson
  oauth2
'''

__author__ = "Konpaku Kogasa, Hameedullah Khan"
__version__ = "0.1"

# Library modules
import urllib2
import time

# In Python2.6 the parse_qsl() function is located in the urlparse library.  In earlier versions it was part of the cgi library.
# Use "import cgi as urlparse" for Python2.5 and earlier, use "import urlparse" for Python2.6
# Swapping these two lines is all that is required to make the code compatible with the other version

#import cgi as urlparse
import urlparse

# Non library modules
import simplejson
import oauth2 as oauth

# Taken from oauth implementation at: http://github.com/harperreed/twitteroauth-python/tree/master
REQUEST_TOKEN_URL = 'https://api.twitter.com/oauth/request_token'
ACCESS_TOKEN_URL = 'https://api.twitter.com/oauth/access_token'
AUTHORIZATION_URL = 'https://api.twitter.com/oauth/authorize'
SIGNIN_URL = 'https://api.twitter.com/oauth/authenticate'


class OAuthApi:
    def __init__(self, consumer_key, consumer_secret, token=None, token_secret=None):
    	if token and token_secret:
    		token = oauth.Token(token, token_secret)
    	else:
    		 token = None
        self._Consumer = oauth.Consumer(consumer_key, consumer_secret)
        self._signature_method = oauth.SignatureMethod_HMAC_SHA1()
        self._access_token = token 

    def _GetOpener(self):
        opener = urllib2.build_opener()
        return opener
        
    def _FetchUrl(self,
                    url,
                    http_method=None,
                    parameters=None):
        '''Fetch a URL, optionally caching for a specified time.
    
        Args:
          url: The URL to retrieve
          http_method: 
          	One of "GET" or "POST" to state which kind 
          	of http call is being made
          parameters:
            A dict whose key/value pairs should encoded and added 
            to the query string, or generated into post data. [OPTIONAL]
            depending on the http_method parameter
    
        Returns:
          A string containing the body of the response.
        '''
        # Build the extra parameters dict
        extra_params = {}
        if parameters:
          extra_params.update(parameters)
        
        req = self._makeOAuthRequest(url, params=extra_params, 
                                                    http_method=http_method)
        
        # Get a url opener that can handle Oauth basic auth
        opener = self._GetOpener()

        if http_method == "POST":
            encoded_post_data = req.to_postdata()
            # Removed the following line due to the fact that OAuth2 request objects do not have this function
            # This does not appear to have any adverse impact on the operation of the toolset
            #url = req.get_normalized_http_url()
        else:
            url = req.to_url()
            encoded_post_data = ""
            
        if encoded_post_data:
        	url_data = opener.open(url, encoded_post_data).read()
        else:
        	url_data = opener.open(url).read()
        opener.close()
    
        # Always return the latest version
        return url_data
    
    def _makeOAuthRequest(self, url, token=None,
                                        params=None, http_method="GET"):
        '''Make a OAuth request from url and parameters
        
        Args:
          url: The Url to use for creating OAuth Request
          parameters:
             The URL parameters
          http_method:
             The HTTP method to use
        Returns:
          A OAauthRequest object
        '''
        
        oauth_base_params = {
        'oauth_version': "1.0",
        'oauth_nonce': oauth.generate_nonce(),
        'oauth_timestamp': int(time.time())
        }
        
        if params:
            params.update(oauth_base_params)
        else:
            params = oauth_base_params
        
        if not token:
            token = self._access_token
        request = oauth.Request(method=http_method,url=url,parameters=params)
        request.sign_request(self._signature_method, self._Consumer, token)
        return request

    def getAuthorizationURL(self, token, url=AUTHORIZATION_URL):
        '''Create a signed authorization URL
        
        Authorization provides the user with a VERIFIER which they may in turn provide to
        the consumer.  This key authorizes access.  Used primarily for clients.
        
        Returns:
          A signed OAuthRequest authorization URL 
        '''
        return "%s?oauth_token=%s" % (url, token['oauth_token'])

    def getAuthenticationURL(self, token, url=SIGNIN_URL, force_login=False):
        '''Create a signed authentication URL
        
        Authentication allows a user to directly authorize Twitter access with a click.
        Used primarily for web-apps.
        
        Returns:
          A signed OAuthRequest authentication URL
        '''
        auth_url = "%s?oauth_token=%s" % (url, token['oauth_token'])
        if force_login:
            auth_url += "&force_login=1"
        return auth_url
        
    def getRequestToken(self, url=REQUEST_TOKEN_URL):
        '''Get a Request Token from Twitter
        
        Returns:
          A OAuthToken object containing a request token
        '''
        resp, content = oauth.Client(self._Consumer).request(url, "GET")
        if resp['status'] != '200':
            raise Exception("Invalid response %s." % resp['status'])

        return dict(urlparse.parse_qsl(content))
    
    def getAccessToken(self, token, verifier=None, url=ACCESS_TOKEN_URL):
        '''Get a Request Token from Twitter
        
        Note: Verifier is required if you AUTHORIZED, it can be skipped if you AUTHENTICATED
        
        Returns:
          A OAuthToken object containing a request token
        '''
        token = oauth.Token(token['oauth_token'], token['oauth_token_secret'])
        if verifier:
            token.set_verifier(verifier)
        client = oauth.Client(self._Consumer, token)
        
        resp, content = client.request(url, "POST")
        return dict(urlparse.parse_qsl(content))
    
    def FollowUser(self, user_id, options = {}):
        '''Follow a user
         Args:
        user_id: The id or screen name of the user to follow
        options:
              A dict of options for the friendships/create call.
              See the link below for what options can be passed
              http://apiwiki.twitter.com/Twitter-REST-API-Method%3A-friendships%C2%A0create           
        '''
        options['id'] = user_id
        return self.ApiCall("friendships/create", "POST", options)

    def UnfollowUser(self, user_id, options ={}):
        '''Stop following a user
         Args:
        user_id: The id or screen name of the user to follow
        options:
              A dict of options for the friendships/destroy call.
              See the link below for what options can be passed
              http://apiwiki.twitter.com/Twitter-REST-API-Method%3A-friendships%C2%A0destroy
        '''
        options['id'] = user_id
        return self.ApiCall("friendships/destroy", "POST", options)
        
    def GetFriends(self, options={}):
    	'''Return a list of users you are following
    	
    	Args:
    	options:
          	A dict of options for the statuses/friends call.
          	See the link below for what options can be passed
          	http://apiwiki.twitter.com/Twitter-REST-API-Method%3A-statuses%C2%A0friends	

    	options['cursor']:
    		By default twitter returns a list of 100
    		followers. If you have more, you will need to
    		use the cursor value to paginate the results.
    		A value of -1 means to get the first page of results.
    		
    		the returned data will have next_cursor and previous_cursor
    		to help you continue pagination          	
    		
        Return: Up to 100 friends in dict format
    	'''
    	return self.ApiCall("statuses/friends", "GET", options)
    	
    def GetFriendsIDs(self, options={}):
    	'''Return a list of users IDs you are following

    	Args:
    	options:
          	A dict of options for the friends/ids call.
          	See the link below for what options can be passed
          	http://apiwiki.twitter.com/Twitter-REST-API-Method:-friends%C2%A0ids	

    	options['cursor']:
    		By default twitter returns a list of 5000
    		friends' IDs. If you have more, you will need to
    		use the cursor value to paginate the results.
    		A value of -1 means to get the first page of results.

    		the returned data will have next_cursor and previous_cursor
    		to help you continue pagination          	

        Return: Up to 5000 friends IDs in dict format
    	'''
    	return self.ApiCall("friends/ids", "GET", options)
    
    def GetFollowers(self, options={}):
    	'''Return followers
    	
    	Args:
    	options:
          	A dict of options for the statuses/followers call.
          	See the link below for what options can be passed
          	http://apiwiki.twitter.com/Twitter-REST-API-Method%3A-statuses%C2%A0followers
          	
          	
    	options['cursor']:
    		By default twitter returns a list of 100
    		followers. If you have more, you will need to
    		use the cursor value to paginate the results.
    		A value of -1 means to get the first page of results.
    		
    		the returned data will have next_cursor and previous_cursor
    		to help you continue pagination
    		          		
        Return: Up to 100 followers in dict format
    	'''
    	return self.ApiCall("statuses/followers", "GET", options)

    def GetFollowersIDs(self, options={}):
    	'''Returns an array of numeric IDs for every user following the specified user.

    	Args:
    	options:
          	A dict of options for the followers/ids call.
          	See the link below for what options can be passed
          	http://apiwiki.twitter.com/Twitter-REST-API-Method:-followers%C2%A0ids	

    	options['cursor']:
    		By default twitter returns a list of 5000
    		followers'' IDs. If you have more, you will need to
    		use the cursor value to paginate the results.
    		A value of -1 means to get the first page of results.

    		the returned data will have next_cursor and previous_cursor
    		to help you continue pagination          	

        Return: Up to 5000 followers' IDs in dict format
    	'''
    	return self.ApiCall("followers/ids", "GET", options)
    
    def GetFriendsTimeline(self, options = {}):
    	'''Get the friends timeline. Does not contain retweets.
    	
          Args:
          options:
          	A dict of options for the statuses/friends_timeline call.
          	See the link below for what options can be passed
          	http://apiwiki.twitter.com/Twitter-REST-API-Method%3A-statuses-friends_timeline	
         
          Return: The friends timeline in dict format
    	'''
    	return self.ApiCall("statuses/friends_timeline", "GET", options)
    
    def GetHomeTimeline(self, options={}):
    	'''Get the home timeline. Unlike friends timeline it also contains retweets
    	
          Args:
          options:
          	A dict of options for the statuses/home_timeline call.
          	See the link below for what options can be passed
          	http://apiwiki.twitter.com/Twitter-REST-API-Method%3A-statuses-home_timeline
          	
          Return: The home timeline in dict format	
    	'''
    	return self.ApiCall("statuses/home_timeline", "GET", options)    
    
    def GetUserTimeline(self, options={}):
    	'''Get the user timeline. These are tweets just by a user, and do not contain retweets
    	
          Args:
          options:
          	A dict of options for the statuses/user_timeline call.
          	See the link below for what options can be passed
          	http://apiwiki.twitter.com/Twitter-REST-API-Method%3A-statuses-user_timeline
          	
          Return: The home timeline in dict format	
    	'''
    	return self.ApiCall("statuses/user_timeline", "GET", options)    
    
    def GetPublicTimeline(self):
    	'''
    		Get the public timeline, which is the 20 most recent statuses from non-protected
    		and custom icon users.  According to the API docs, this is cached for 60 seconds.
          	
          Return: The public timeline in dict format	
    	'''
    	return self.ApiCall("statuses/public_timeline", "GET", {})     
    
    def UpdateStatus(self, status, options = {}):
    	'''
        Args:
          status: The status you wish to update to
          options:
          	A dict of options for the statuses/update call.
          	See the link below for what options can be passed
          	http://apiwiki.twitter.com/Twitter-REST-API-Method%3A-statuses%C2%A0update
        Returns:
          Whether or not the status update suceeded
    	'''
    	options['status'] = status
    	return self.ApiCall("statuses/update", "POST", options)

    def GetDirectMessages(self, options={}):
    	'''Returns a list of the 20 most recent direct messages sent to the authenticating user.

          Args:
          options:
          	A dict of options for the direct_messages call.
          	See the link below for what options can be passed
          	http://dev.twitter.com/doc/get/direct_messages

          Return: The direct messages in dict format	
    	'''
    	return self.ApiCall("direct_messages", "GET", options)

    def GetDirectMessagesSent(self, options={}):
    	'''Returns a list of the 20 most recent direct messages sent by the authenticating user.

          Args:
          options:
          	A dict of options for the direct_messages/sent call.
          	See the link below for what options can be passed
          	http://dev.twitter.com/doc/get/direct_messages

          Return: The direct messages in dict format	
    	'''
    	return self.ApiCall("direct_messages/sent", "GET", options)

    def GetMentions(self, options={}):
    	'''Get mentions (@user) of this user.
    	
          Args:
          options:
          	A dict of options for the statuses/user_timeline call.
          	See the link below for what options can be passed
          	http://apiwiki.twitter.com/Twitter-REST-API-Method%3A-statuses-mentions
          	
          Return: Return the mentions in dict format
    	'''
    	return self.ApiCall("statuses/mentions", "GET", options)
        
    def Retweet(self, id, options={}):
    	'''Retweets the given tweet
        
        Args:
          id: The integer id of the tweet to be retweeted
          options:
          	A dict of options for the statuses/retweet call.
          	See the link below for what options can be passed
          	http://apiwiki.twitter.com/Twitter-REST-API-Method%3A-statuses-retweet
        Returns:
          Whether or not the retweet succeeded
    	'''
    	#options['id'] = id
    	return self.ApiCall("statuses/retweet/%s" % (id), "POST", options)
        
    def SendDM(self, user, text, options={}):
    	'''Send DM to specified user
        
        Args:
          user: The id or screen name of the recipient of the DM
          text: The text of the DM to be sent
          options:
          	A dict of options for the statuses/update call.
          	See the link below for what options can be passed
          	http://apiwiki.twitter.com/Twitter-REST-API-Method%3A-statuses-retweet
        Returns:
          Whether or not the retweet suceeded
    	'''
    	options['user'] = user
        options['text'] = text
    	return self.ApiCall("direct_messages/new", "POST", options)

    def VerifyCredentials(self, options={}):
    	'''Verifies that the credential set being used is valid
    	
          Args:
          options:
          	A dict of options for the account/verify_credentials call.
          	See the link below for what options can be passed
          	http://apiwiki.twitter.com/Twitter-REST-API-Method%3A-account%C2%A0verify_credentials
          	
          Return: Return the user's info in dict format
    	'''
    	return self.ApiCall("account/verify_credentials", "GET", options)
        
    def GetRateLimitStatus(self, options={}):
    	'''Checks to see how many API calls you have left this hour
    	
          Args:
          options:
          	A dict of options for the account/rate_limit_status call.
          	See the link below for what options can be passed
          	http://apiwiki.twitter.com/Twitter-REST-API-Method%3A-account%C2%A0rate_limit_status
          	
          Return: Return the user's info in dict format
    	'''
    	return self.ApiCall("account/rate_limit_status", "GET", options)
        
    def GetRelationship(self, user_id = None, screen_name = None, options={}):
        '''Get the relationship between the authenticated user of this API and the specified user
         Args:
        user_id: The user_id of the user to target.  (Source is automatically the authenticated user in this case.)
        screen_name: The screen name of the user to target.  (Source is automatically the authenticated user in this case.)
          NOTE!  One of thse two MUST be specified.
        options:
              A dict of options for the friendships/show call.
              See the link below for what options can be passed
              http://apiwiki.twitter.com/Twitter-REST-API-Method%3A-friendships-show
        '''
        if user_id:
            options['target_id'] = user_id
        if screen_name:
            options['target_screen_name'] = screen_name
        return self.ApiCall("friendships/show", "GET", options)
        
    def GetUsersShow(self, options={}):
      '''Returns extended information of a given user, specified by ID or screen name as per the required id parameter. The author's most recent status will be returned inline.

          Args:
          options:
              A dict of options for the users/show call.
              See the link below for what options can be passed
              http://dev.twitter.com/doc/get/users/show

          Return: The home timeline in dict format    
      '''
      return self.ApiCall("users/show", "GET", options)
        
    def ApiCall(self, call, type="GET", parameters={}):
        '''Calls the twitter API
        
       Args:
          call: The name of the api call (ie. account/rate_limit_status)
          type: One of "GET" or "POST"
          parameters: Parameters to pass to the Twitter API call
        Returns:
          Returns the twitter.User object
        '''
        return_value = []
          # We use this try block to make the request in case we run into one of Twitter's many 503 (temporarily unavailable) errors.
          # Other error handling may end up being useful as well.
        try:
            json = self._FetchUrl("https://api.twitter.com/1/" + call + ".json", type, parameters)
          # This is the most common error type you'll get.  Twitter is good about returning codes, too
          # Chances are that most of the time you run into this, it's going to be a 503 "service temporarily unavailable".  That's a fail whale.
        except urllib2.HTTPError, e:
            return e
          # Getting an URLError usually means you didn't even hit Twitter's servers.  This means something has gone TERRIBLY WRONG somewhere.
        except urllib2.URLError, e:
            return e
        else:
            return simplejson.loads(json)
