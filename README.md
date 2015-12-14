## tweepy_trawler.py
Script which hits the Twitter public API and performs simple network analysis on a list of targeted twitter handles. 

### ARGS
#### json file with API credentials
ex:
{
  "consumer_key" : "your_key",
  "consumer_secret" : "your_secret",
  "access_token" : "your_access_token",
  "access_token_secret" : "your_access_token_secret"
}

#### target list
newline delimited file of twitter handles without the @ symbol
ex:
<br>twitter_user342
<br>fake_handle2342
<br>foo_bar

#### output file
name of the output csv file

ex:

output.csv



#### dependencies
current iteration expects a folder called profile-images to exist in the calling directory to store off target profile images.  Will change that in the future, I think.
several libraries:

    import csv
    
    import sys
    
    import json
    
    import tweepy
    
    import urllib
    
    import argparse
    
    import itertools
    
    from itertools import izip_longest
    
    from kitchen.text.converters import to_bytes 

I think Kitchen and tweepy are the only two that need to be pip installed.

##### note: worth grabbing the troll_soup repo if you want to use tweepy_trawler for ff.
