'''
hits the twitter public api to gather data and build the network that links a list of known/suspected targets
returns a csv that is front-end importable to Gotham.
with appropriate modifications to the ontology, can display and manipulate twitter network in the graph app
author: joshua owens
'''
import os
import csv
import sys
import json
import tweepy
import urllib
import argparse
import itertools
from itertools import izip_longest
from tweepy.error import TweepError
from kitchen.text.converters import to_bytes 

# ARGVARS
_API_CREDS_ARGVAR = 'api_creds'
_TARGET_LIST_ARGVAR = 'target_list'
_OUTPUT_FILE_ARGVAR = 'output_file'
_ONLY_TARGET_ARGVAR = 'only_targets'

# API creds
_CONSUMER_KEY = "consumer_key"
_CONSUMER_SECRET = "consumer_secret"
_ACCESS_TOKEN = "access_token"
_ACCESS_TOKEN_SECRET = "access_token_secret"

# dataset constants
_FOLLOWED_BY_KEY = "followed_by"
_FOLLOWS_KEY = "follows"
_IS_FRIEND = "friend_of"
_IS_FOLLOWER = "follower_of"
_IS_FRIEND_AND_FOLLOWER = "strongly_connected"
_IMG_DIR = './profile-images/'

# header constants
_ID = 'twitter_id'
_SN = 'twitter_screen_name'
_NAME = 'name'
_FRIEND_C = 'friend_count'
_FOLLOWER_C = 'follower_count'
_CREATED_DTM = 'created_date'
_GEO_ENABLED = 'geo_enabled'
_LOC = 'location'
_TZ = 'time_zone'
_UTC = 'utc_offset'
_DESC = 'description'
_URL = 'url'
_IMG_URL = 'profile_image_url'
_IS_TGT = 'is_target'
_STATUS_COORDS = 'status_geocoords'
_STATUS_DTM = 'status_created_datetime'
_STATUS_HASHTAGS = 'status_hashtags'
_STATUS_USER_MENT = 'status_user_mentions'
_STATUS_GEO = 'status_geo'
_STATUS_REPLY_TO = 'status_in_reply_to'
_STATUS_LOC_NAME = 'status_location_name'
_STATUS_LOC_COORDS = 'status_bounding_coords'
_STATUS_SOURCE = 'status_source'
_STATUS_TEXT = 'status_text'

# csv header
_HEADER = [_ID,_SN,_NAME,_FRIEND_C,_FOLLOWER_C,_CREATED_DTM,_GEO_ENABLED,_LOC,_TZ,_UTC,_DESC,_URL,_IMG_URL,_IS_TGT,_FOLLOWED_BY_KEY,_FOLLOWS_KEY,
    _STATUS_COORDS,_STATUS_DTM,_STATUS_HASHTAGS,_STATUS_USER_MENT,_STATUS_GEO,_STATUS_REPLY_TO,_STATUS_LOC_NAME,_STATUS_LOC_COORDS,_STATUS_SOURCE,_STATUS_TEXT]

def _parse_args(argv):
    # Parses args into a dict of ARGVAR=value, or None if the argument wasn't supplied 
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(_API_CREDS_ARGVAR, metavar="<twitter api cred filename>", help="json file with creds for twitter api")
    parser.add_argument(_TARGET_LIST_ARGVAR, metavar="<target list filename>", help="path to list of target screen names")
    parser.add_argument(_OUTPUT_FILE_ARGVAR, metavar="<output filename>", help="output directory and filename")
    parser.add_argument("-t", "--targets", dest=_ONLY_TARGET_ARGVAR, action="store_true", default=False, help="set this if you only wish to return the target network - NO other users will be returned")
    return vars(parser.parse_args(argv))
 
def _print_error(msg):
    sys.stderr.write('Error: ' + msg + '\n')
 
def _validate_args(args):
    ''' Performs validation on the given args dict, returning a non-zero exit code if errors were found or None if all is well '''
    return None 

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)
    
def main(argv):
    # get args
    args = _parse_args(map(str, argv))
    err = _validate_args(args)
    if err is not None:
        return err
    
    # option to only return the explicitly listed target network
    only_targets = args[_ONLY_TARGET_ARGVAR]

    # open output file and write header
    out = open(args[_OUTPUT_FILE_ARGVAR],'w')    
    writer = csv.DictWriter(out,fieldnames=_HEADER)
    writer.writeheader()

    # pull in json and retreive api creds
    api_file = args[_API_CREDS_ARGVAR]
    with open(api_file,'r') as creds:
      cred_dict = json.load(creds)
   
    ## build auth
    auth = tweepy.OAuthHandler(cred_dict[_CONSUMER_KEY], cred_dict[_CONSUMER_SECRET]) 
    auth.set_access_token(cred_dict[_ACCESS_TOKEN], cred_dict[_ACCESS_TOKEN_SECRET])

    ## create tweepy API object that will alert and wait when hitting the rate limit ##
    api = tweepy.API(auth,timeout=720,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)
    
    ## global id_pool, return_set, and target_ids.
    # all-target id pool with links to their connected target(s)
    id_pool={}
    # the unique collection of their network nodes
    return_set=set()
    # persistant list of target ids
    target_set=set()
    
    # make a directory for profile images for this pull in the current directory
    if not os.path.exists(_IMG_DIR):
      os.makedirs(_IMG_DIR)
    
    ## open the file of target handles and collect relevant data 
    with open(args[_TARGET_LIST_ARGVAR],'r') as target_file:
      
      for target_list in grouper(target_file,100):
        request_str=','.join(filter(lambda item: item is not None, target_list))
        targets = api.lookup_users(screen_names=[request_str])
        
        for target_user in targets:
          # don't process targets with protected data
          if target_user.protected:
            continue

          # save off target profile image in a decent resolution and add target id to the set of all targets and the return set
          target_id = target_user.id_str
          target_sn = target_user.screen_name
          urllib.urlretrieve(target_user.profile_image_url.replace('normal','400x400'),_IMG_DIR+target_sn+'_profile_image.jpg')
          target_set.add(target_id)
          return_set.add(target_id)
          # intermediary dict (per-target)
          ff_dict={}
          
          # we haven't forgotten about you - just processing
          print('processing {0}'.format(unicode(target_sn)))
        
          # the value stored at key=target_user.id_str is a dictionary with two empty sets.
          if target_id not in id_pool: 
            id_pool[target_id]={_FOLLOWED_BY_KEY:[],_FOLLOWS_KEY:[]}

          # get all friends, allowing tweepy to manage pagination and rate limiting
          for friend_id in tweepy.Cursor(api.friends_ids,screen_name=target_sn).items():
            # use strings instead of ints.  Things get weird with ints.
            friend_id=str(friend_id)
            # is at least a friend
            ff_dict[friend_id]=_IS_FRIEND

          for follower_id in tweepy.Cursor(api.followers_ids,screen_name=target_sn).items():
            follower_id=str(follower_id)
            # if already a friend, make strongly connected
            if follower_id in ff_dict:
              ff_dict[follower_id]=_IS_FRIEND_AND_FOLLOWER
            # otherwise, just a follower
            else:
              ff_dict[follower_id]=_IS_FOLLOWER
          
          for user_id in ff_dict:
            # if they've already been added to the global id_pool, then they are a connecting node between two targets.
            # add them to the return set.
            if user_id in id_pool:
              return_set.add(user_id)
            # otherwise, create a new entry in the id_pool to hold their connections
            else:
              id_pool[user_id]={_FOLLOWED_BY_KEY:[],_FOLLOWS_KEY:[]}

            # mark the current target as a follower of this user if appropriate
            if ff_dict[user_id]==_IS_FRIEND or ff_dict[user_id]==_IS_FRIEND_AND_FOLLOWER:
              id_pool[user_id][_FOLLOWED_BY_KEY].append(target_id)
            # mark the current target as followed by this user if appropriate
            if ff_dict[user_id]==_IS_FOLLOWER or ff_dict[user_id]==_IS_FRIEND_AND_FOLLOWER:
              id_pool[user_id][_FOLLOWS_KEY].append(target_id)


      ## --- ALL TARGETS PROCESSED --- ##
      
      # quick escape if you're only looking for the network amongst your target group.
      # if you want to save twitter api hits and some import time/bloat, you can opt to
      # only return your target network
      if only_targets:
        return_set=target_set

      ## take all users in the return_set and process user data into rows for csv
      # Twitter API will accept blocks of <= 100 user ids for batched user lookups
      for user_list in grouper(return_set,100):
        request_str=','.join(filter(lambda item: item is not None, user_list))
        users = api.lookup_users(user_ids=[request_str])

        ## For now, the best solution is to write a row per connection.  Probably fine? Might change.
        for user in users:
          ## Processing PERSONAL data
          print_dict = {
            _ID : user.id_str,
            _SN : user.screen_name,
            _NAME : to_bytes(user.name.replace('\n','\t').replace('\r','\t')),
            _FRIEND_C : str(user.friends_count),
            _FOLLOWER_C : str(user.followers_count),
            _CREATED_DTM : str(user.created_at),
            _GEO_ENABLED : str(user.geo_enabled),
            _LOC : to_bytes(user.location.replace('\n','\t').replace('\r','\t')),
            _TZ : user.time_zone,
            _UTC : user.utc_offset,
            _DESC : to_bytes(user.description.replace('\n','\t').replace('\r','\t')),
            _URL : user.url,
            _IMG_URL : user.profile_image_url.replace('normal','400x400'),
            _IS_TGT : str(user.id_str in target_set)
          }
          
          ## Processing STATUS data
          try:
            tags=[]
            mentions=[]

            print_dict[_STATUS_COORDS]=str(user.status.coordinates)
            print_dict[_STATUS_DTM]=str(user.status.created_at)
            for tag in user.status.entities['hashtags']:
              tags.append(to_bytes(tag['text']))
            print_dict[_STATUS_HASHTAGS]='|'.join(tags)
            for mention in user.status.entities['user_mentions']:
              mentions.append(to_bytes(mention['id_str']+'|'+mention['screen_name']+'|'+mention['name']))
            print_dict[_STATUS_USER_MENT]='|'.join(mentions)
            print_dict[_STATUS_GEO]=str(user.status.geo)
            print_dict[_STATUS_REPLY_TO]=to_bytes(user.status.in_reply_to_screen_name)
            if user.status.place:
              coord_str=''
              for point in user.status.place.bounding_box.coordinates[0]:
                coord_str+=str(point).replace(',',':')
              print_dict[_STATUS_LOC_COORDS]=coord_str
              print_dict[_STATUS_LOC_NAME]=user.status.place.full_name.replace(',','')
            print_dict[_STATUS_SOURCE]=to_bytes(user.status.source)
            print_dict[_STATUS_TEXT]=to_bytes(user.status.text.replace(',','').replace('\n','\t').replace('\r','\t'))
          except TweepError, te:
            print('error processing status for user {0}: {1}'.format(unicode(user.screen_name),unicode(te)))
            continue
        
          # catch error when user has no current status
          except AttributeError, ae:
            print('error processing status from {0}: {1}'.format(unicode(user.screen_name),unicode(ae)))
            continue
          
          ## write the user to file without connections
          try:
            writer.writerow(print_dict)
          except Exception, e:
            print(e)
            continue

          # write user again with each of their followers as its own line
          for follower in id_pool[user.id_str][_FOLLOWED_BY_KEY]:
            try:
              print_dict[_FOLLOWED_BY_KEY]=follower
              writer.writerow(print_dict)
            except TweepError, te:
              print('error processing {0} follower: {1}: {2}'.format(unicode(user.screen_name),unicode(follower),unicode(te)))
              continue
          
          # get rid of the followed by key
          if _FOLLOWED_BY_KEY in print_dict:
            print_dict.pop(_FOLLOWED_BY_KEY,None)

          # write user again with each of their friends as its own line
          for friend in id_pool[user.id_str][_FOLLOWS_KEY]: 
            try:
              print_dict[_FOLLOWS_KEY]=friend
              writer.writerow(print_dict)
            except Exception, e:
              print(e)
              continue


    # Close output file
    out.close()
    return 0
 
if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
