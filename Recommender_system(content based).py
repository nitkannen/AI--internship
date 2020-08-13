#!/usr/bin/env python
# coding: utf-8

# In[360]:


import pymongo
from pymongo import MongoClient
import pprint

import pandas as pd
import numpy as np
import subprocess
from numpy import dot
from numpy.linalg import norm

#bson to json
from bson.objectid import ObjectId
from bson.json_util import loads, dumps

import codecs
import sys
import json
import dns
import os


# In[361]:
    

user_fbid = str(sys.argv[1])
count = int(sys.argv[2])
batch_size = int(sys.argv[3])


# In[362]:
ENV_VARIABLE= str(os.environ.get('USER'))

if(ENV_VARIABLE == "production"):
    pass
    #client =  MongoClient(production link)
    #db = client['prodsing']
    #fbusers = db['fbUser']
    
else:
    client =  MongoClient('add connection link')
    db = client['gogaga']
    fbusers = db['fbUser']


# In[363]:


u = fbusers.find({
    'facebookId': user_fbid   
})
    
if bool(fbusers.find_one({
    'facebookId': user_fbid   
})):
    use = list(u)
if (len(use) == 0):
    exit()
user_json = dumps(use[0])


# In[364]:


#sys.stdout.write(sorted_output_list)


# In[365]:


#<configuration>
#  <connectionStrings>
 #   <add name="myConnectionString"
  #  connectionString="client='mongodb+srv://dbuser:pass@gogaga-jvfdv.mongodb.net';database='gogaga';collection = 'fbuser'" />
  #</connectionStrings>
#</configuration> 


# In[366]:


#string connStr = ConfigurationManager.ConnectionStrings["myConnectionString"].ConnectionString;


# In[367]:


number_of_prospects = 3 #### Number of prosects needed from interest compatitibility
number_of_taste_prospects = 2 ## Number of prospects needed from taste compatibility


threshold_score = 0.4 # Can be adjusted, optimal value 0.3 for five prospects or below
taste_threshold_score = 0.4 # Threshold can be modified



# In[368]:
## The doc string below contains the XML format for options available
## to the user in APP UI
s = '''
    <head>
    <playlist>
    <item>On my playlist, you will find...</item>
    <item>Bollywood</item>
    <item>Classical</item>
    <item>HipHop</item>
    <item>Jazz</item>
    <item>Country</item>
    <item>Electronic</item>
    <item>Rock</item>
    <item>World Music</item>

    <item>मेरे फ़ोन पर आप इस प्रकार के गाने पाएंगे...</item>
    <item>बॉलीवुड</item>
    <item>क्षेत्रीय/शास्त्रीय</item>
    <item>हिप हॉप</item>
    <item>जैज़</item>
    <item>पश्चिमी</item>
    <item>इलेक्ट्रोनिक</item>
    <item>रॉक</item>
    <item>विदेशी संगीत</item>

    </playlist>
    <vacation>
    <item>On a vacation I am...</item>
    <item>Hiker and backpacker</item>
    <item>Adrenaline junkie</item>
    <item>Beach Goer</item>
    <item>Road tripper</item>
    <item>History lover</item>
    <item>Luxury traveller</item>
    <item>Nature person</item>
    <item>Wildlife enthusiast</item>

    <item>मुझे इस तरह छुट्टी पसंद है...</item>
    <item>पहाड़ों पर और बैकपैकिंग</item>
    <item>एडवेंचर और साहसी</item>
    <item>समुद्र तट और लहरें</item>
    <item>सड़क यात्राएं</item>
    <item>इतिहास और संग्रहालय</item>
    <item>लक्जरी यात्राएं</item>
    <item>प्राकृतिक दृश्य</item>
    <item>वन्यजीव देखने</item>

    </vacation>

    <party>
    <item>At a party, I am a...</item>
    <item>Beer guzzler</item>
    <item>Whisky lover</item>
    <item>Wine sipper</item>
    <item>Cocktail picker</item>
    <item>Coffee lover</item>
    <item>Chai fan</item>
    <item>Teetotaler</item>

    <item>एक पार्टी पर मैं, यह पसंद करता हूँ</item>
    <item>बीयर</item>
    <item>व्हिस्की</item>
    <item>वाइन</item>
    <item>कॉकटेल</item>
    <item>कॉफी प्रेमी</item>
    <item>चाय शौक़ीन</item>
    <item>टी-टोटलेर</item>
    </party>

    <food>
    <item>On a regular basis, I am a...</item>
    <item>Vegetarian</item>
    <item>Non Vegetarian</item>
    <item>Strictly Vegan</item>
    <item>Low on calories</item>
    <item>Junk food lover</item>

    <item>नियमित आधार पर, मैं हूँ...</item>
    <item>शाकाहारी (अंडा चलता है)</item>
    <item>मांसाहारी</item>
    <item>शुद्ध शाकाहारी</item>
    <item>काम कैलोरी वाला खाना</item>
    <item>जंक फूड प्रेमी</item>

    </food>
    <fitness>
    <item>For fitness, I do the following...</item>
    <item>Gym</item>
    <item>Running</item>
    <item>Diet control</item>
    <item>Yoga</item>
    <item>Motivational videos</item>
    <item>Laziness rules</item>

    <item>स्वास्थ के लिए, मैं निम्न कार्य करता हूं...</item>
    <item>व्यायामशाला</item>
    <item>दौड़ / टहलना</item>
    <item>आहार नियंत्रण</item>
    <item>योगा</item>
    <item>प्रेरणात्मक वीडियो</item>
    <item>आलस्य पसंद</item>

    </fitness>
    <sports>
    <item>When it comes to sports, I follow...</item>
    <item>Cricket</item>
    <item>Football</item>
    <item>Basketball</item>
    <item>Racket Sports</item>
    <item>Motor Sports</item>
    <item>Water Sports</item>
    <item>Golf</item>

    <item>जब खेलों की बात आती है, तो मैं अनुसरण करता हूं...</item>
    <item>क्रिकेट</item>
    <item>फ़ुटबॉल</item>
    <item>बास्केटबाल</item>
    <item>रैकेट के खेल</item>
    <item>मोटर स्पोर्ट्स</item>
    <item>पानी के खेल</item>
    <item>गोल्फ़</item>
    </sports>

    <personality>
    <item>I think I am...</item>
    <item>Introvert</item>
    <item>Extrovert</item>
    <item>GoGetter</item>
    <item>Adventurous</item>
    <item>Socially Dormant</item>
    <item>Witty</item>
    <item>Sapio sexual</item>

    <item>मेरा व्यक्तित्व प्रकार...</item>
    <item>अंतर्मुखी</item>
    <item>बहिर्मुखी</item>
    <item>चीजों को हासिल करो</item>
    <item>साहसिक</item>
    <item>सामाजिक रूप से निष्क्रिय</item>
    <item>मज़ाकिया</item>
    <item>बुद्धिमत लोग पसंद है</item>
    </personality>
    <art>
    <item>When it comes to arts, I am...</item>
    <item>Artist</item>
    <item>Painter/Sketcher</item>
    <item>Photographer</item>
    <item>Blogger</item>
    <item>Master chef</item>
    <item>Singer</item>
    <item>Musician</item>
    <item>Dance Machine</item>
    <item>Writer</item>

    <item>जब कला की बात आती है, मैं हूँ...</item>
    <item>कलाकार</item>
    <item>पेंटर / स्केचर</item>
    <item>फोटोग्राफर</item>
    <item>ब्लॉगर</item>
    <item>खाना बनाने में सरताज</item>
    <item>गायक</item>
    <item>संगीतकार</item>
    <item>नर्तक</item>
    <item>लेखक</item>

    </art>
    <books>
    <item>When it comes to books</item>
    <item>Read everyday</item>
    <item>Read occasionally</item>
    <item>Not a reader</item>

    <item>जब किताबों की बात आती है</item>
    <item>हर रोज पढ़ना</item>
    <item>कभी-कभार पढ़ना</item>
    <item>पढ़ना पसंद नहीं</item>
    </books>
    <movie>
    <item>In movies, I like...</item>
    <item>Romantic</item>
    <item>Comedy</item>
    <item>Action</item>
    <item>Thriller</item>
    <item>Horror</item>
    <item>Sci-fi</item>

    <item>फिल्मों में, मुझे पसंद है..</item>
    <item>प्रेम प्रसंगयुक्त</item>
    <item>हास्यास्पद</item>
    <item>लड़ाई/एक्शन</item>
    <item>थ्रिलर</item>
    <item>डरावनी</item>
    <item>कल्पित विज्ञान</item>
    </movie>

    <animals>
    <item>I love...</item>
    <item>Dogs</item>
    <item>Cats</item>
    <item>Birds</item>
    <item>Aquatic Animals</item>
    <item>Dislike Animals</item>

    <item>मुझे पसंद हैं...</item>
    <item>कुत्ते</item>
    <item>बिल्लियाँ</item>
    <item>पक्षी</item>
    <item>जलीय जानवर</item>
    <item>जानवर पसंद नहीं</item>
    </animals>

    <timepass>
    <item>In my free time, I will be enjoying...</item>
    <item>Standup comedy</item>
    <item>News</item>
    <item>TV series</item>
    <item>Movies</item>
    <item>Podcast</item>
    <item>Latest Trends</item>

    <item>हास्य वीडियो</item>
    <item>समाचार</item>
    <item>टीवी श्रृंखला</item>
    <item>चलचित्र</item>
    <item>पॉडकास्ट</item>
    <item>नवीनतम रुझान</item>
    </timepass>

    <politics>
    <item>My political sense...</item>
    <item>Apolitical</item>
    <item>Socialism</item>
    <item>Communism</item>
    <item>Capitalism</item>
    <item>Left Wing</item>
    <item>Right Wing</item>

    <item>मेरा राजनीतिक रुझान...</item>
    <item>अराजनैतिक</item>
    <item>समाजवाद</item>
    <item>साम्यवाद</item>
    <item>पूंजीवाद</item>
    <item>वामपन्थी</item>
    <item>दक्षिणपन्थी</item>
    </politics>
    
    </head>
    '''
## The template code below converts the XML format to a usable dictionary    
from xml.etree import cElementTree as ElementTree

class XmlListConfig(list):
    def __init__(self, aList):
        for element in aList:
            if element:
                # treat like dict
                if len(element) == 1 or element[0].tag != element[1].tag:
                    self.append(XmlDictConfig(element))
                # treat like list
                elif element[0].tag == element[1].tag:
                    self.append(XmlListConfig(element))
            elif element.text:
                text = element.text.strip()
                if text:
                    self.append(text)


class XmlDictConfig(dict):
    '''
    Example usage:

    >>> tree = ElementTree.parse('your_file.xml')
    >>> root = tree.getroot()
    >>> xmldict = XmlDictConfig(root)

    Or, if you want to use an XML string:

    >>> root = ElementTree.XML(xml_string)
    >>> xmldict = XmlDictConfig(root)

    And then use xmldict for what it is... a dict.
    '''
    def __init__(self, parent_element):
        if parent_element.items():
            self.update(dict(parent_element.items()))
        for element in parent_element:
            if element:
                # treat like dict - we assume that if the first two tags
                # in a series are different, then they are all different.
                if len(element) == 1 or element[0].tag != element[1].tag:
                    aDict = XmlDictConfig(element)
                # treat like list - we assume that if the first two tags
                # in a series are the same, then the rest are the same.
                else:
                    # here, we put the list in dictionary; the key is the
                    # tag name the list elements all share in common, and
                    # the value is the list itself 
                    aDict = {element[0].tag: XmlListConfig(element)}
                # if the tag has attributes, add those to the dict
                if element.items():
                    aDict.update(dict(element.items()))
                self.update({element.tag: aDict})
            # this assumes that if you've got an attribute in a tag,
            # you won't be having any text. This may or may not be a 
            # good idea -- time will tell. It works for the way we are
            # currently doing XML configuration files...
            elif element.items():
                self.update({element.tag: dict(element.items())})
            # finally, if there are no child tags and no attributes, extract
            # the text
            else:
                self.update({element.tag: element.text})
        
        
        

root = ElementTree.XML(s)
xmldict = XmlDictConfig(root)


for key in xmldict.keys():
    xmldict[key] = xmldict[key]['item'][1:]

#%%
#Called in findProspects()
#This Function invokes cal_score()
#returns a final_list of dictionaries back to findProspects()
def getFinalList(user_json, allUsers, df_filtered,mean_taste_dict):
    """
    this function iterates through the filtered prospect list
    and calculates the compatitbilty score
    
    Args: user_json - The user interests in JSON format
          allUsers - list, this is a list of users in batch
          df_filtered - dataframe, this is a pandas dataframe that is filtered 
                        based on the user preference
          mean_taste_dict - mean of the dictionaries of the user's matches/likes             
    
    returns: list, list of dictionaries containing fbid of prospects and match score
    """
    
    installed_state = 0
    final_list = []
    user_list = loads(user_json)
    # print (df_filtered[{'_id', 'facebookId', 'displayName'}])
    for i in  df_filtered['_id']:
        s = "ObjectId('" + str(i) + "')"
        # print(s)
        prospect = next((user2) for user2 in allUsers if str(user2['_id']) == str(i))
        # print(prospect)
        # print("-----------------------------------------------")
        if ('installed' in prospect):
            installed_state = float(prospect['installed']) #whetther or not the user has installed the app
            #print(installed_state)
        prospect_json = dumps(prospect)
        if(str(user_list['_id']) == str(i)):
            # print('Here')
            interest_match_score = -1
            taste_match_score = -1
            # print(score)
        else:
            interest_match_score = cal_score(prospect_json, user_json,mean_taste_dict,installed_state,0)
            
            taste_match_score = cal_score(prospect_json, user_json,mean_taste_dict,installed_state,1)
            
            
        # print(score)
        prospect['interest_score'] = interest_match_score
        
        prospect['taste_score'] = taste_match_score
        # print(score)
        # print(prospect)
        
        final_list.append(prospect)
    # print(len(final_list))
    # print(final_list[0]['facebookId'])
    return final_list


# In[369]:


#This Function is called by getFinalList()
#It calculates the cosine similarity score between 2 users based on filled 'userInterests'
#returns score
def cal_score(json1, json2, history_dict ,installed_state, use_history):
    """
    This function calculates the compatibility score between 2 users using cosine similarity
    s = a.b/(mod(a)*mod(b))
    
    Args: json1,json2 - user interests in JSON format
          history_dict - this is a dictionary containing the mean history interest
                          of the user
          installed_state- boolean, whether or not the user has app installed to weight the final score
          use_history - boolean, whether to compute taste score or interest score(0/1)
          
    Returns: final score - a weighted value of score between 0 and 1
    """
    


           
    contents1 = json.loads(json1)   # Prospect JSON
      
    content1 = {}
    for item in contents1['userInterests']:
        for key in item.keys():
            content1[key] = item[key]
            
            
            
    if use_history == 1:            # Enters to calculate prospect scores based on history
        
        f2 = history_dict          # Mean dict calculated
        
    else:                      # Enters to calculate prospect score based on interest matching
        
        contents2 = json.loads(json2)  # User Json
        content2 = {}
        for item in contents2['userInterests']:
            for key in item.keys():
                content2[key] = item[key]
                
        f2 = {}
        for key in content2.keys():
            k = len(xmldict[key])
            fv = np.zeros(k)
            for val in content2[key]:
                if val in xmldict[key]:
                    i = xmldict[key].index(val)
                    #print(i)
                    fv[i] = 1
            f2[key] = fv
         
            
    #import numpy as np
    f1 = {}
    for key in content1.keys():
        k = len(xmldict[key])
        fv = np.zeros(k)
        for val in content1[key]:
            if val in xmldict[key]:
                i = xmldict[key].index(val)
                fv[i] = 1
        f1[key] = fv
    
    #print(use_history)    
    #print(f2)
    #print('\n')


    s = 0
    i = 1
    for key in f1.keys():
        if sum(f1[key]) == 0 or sum(f2[key]) == 0:
            s += 0
            #if use_history:
               # print('hey')
        else:
            #if use_history:
                #print('hey')
                #print('key')
            s  += dot(f1[key], f2[key])/(norm(f1[key])*norm(f2[key]))
        i += 1
    # print('result = ', str(s/i))
    final_score = (s/i)*0.85 + 0.15*installed_state
    
    return final_score
#%%

def list_of_liked_users(user_json): 
    """
    This function takes in a user_JSON file containing interests and returns the list
    of liked/connected users in his/her history
    """
    
    likes = set(user_json.get('likes'))# liked by the user
    rejects = set(user_json.get('rejects'))#rejected by the user
    union = [] # Stores connected
    if 'connected' in user_json:
        for a in user_json['connected']:
            #print(type(str(a['charm'])))
            union.append(a.get('charm'))
        
    connected = set(union)  
    # remove the repeating elements
    
    filtered_likes = (likes|connected) - rejects  # removes the rejected overlap from connections|likes
    
    liked_list = []
    for i in range(len(filtered_likes)):
        liked_user = filtered_likes.pop()
        #print(liked_user)
        if len(list(fbusers.find({'facebookId': liked_user})))>0 :
            liked_list.append(list(fbusers.find({'facebookId': liked_user})))
                             
    number_of_liked = len(liked_list)
    #rough_list = (liked_list)
    #return  rough_list
    #print(number_of_liked)
    
    like_list = []
    
    if number_of_liked == 0:
        return like_list
    
    else:
    
        for i in range(number_of_liked):
            content = {}
            #print(i)
            if 'userInterests' in liked_list[i][0]:
                for a in (liked_list[i][0].get('userInterests')):
                    if (a != []) and (a != {}):
                        for key in a.keys():
                            content[key] = a[key]
                         
            if liked_list[i][0].get('userInterests') != []:
                like_list.append(content)
                       
    return (like_list)

#%%
 # This calculates the mean interest score for a user based on history
 # Invoked by history_of_liked_interest
def find_mean_interest_score(like_list):
    """
    This function takes the like list from gunction "list_of_liked_users" and finds
    the mean interest score in order to calculate the user's taste.
    This dictionary of mean taste is used to calculate taste prospects
    Returns: dictionary, mean_like_interst
    """
    
    liked_list = []
    #print(len(like_list))
    for item in like_list:
        f ={}
        for keys in item.keys():
            fv = np.zeros(len(xmldict[keys]))
            for val in item[keys]:
                if val in xmldict[keys]:
                #print(xmldict[keys])
                    i = xmldict[keys].index(val)
                    fv[i] = 1
                
            f[keys] = fv

        liked_list.append(f)

    mean_interest = {}
    for key in xmldict.keys():
        s = 0
        for item in liked_list:
            s += item[key]
            
        if len(liked_list) != 0:
            mean_interest[key] = s/(len(liked_list))
        else:
            mean_interest[key] = np.zeros(len(xmldict[key]))
    #print(mean_interest)    
    return mean_interest # mean 


# In[370]:


def getFilteredUsers(s,preferences,user_dict):
    """
    This function is a basic filter that filters the prospects based on uer preference
    Args: s, list: list of all prospecs
          preferences: users preferences
          
          returns: a filtered list
    """
    filtered_list = []
    
    #Gender check--------------------------------
    if(('men' in preferences) and ('women' in 'preferences')):
        men = preferences['men']
        women = preferences['women']
    elif('men' in 'preferences'):
        men =  preferences['men']                                 # assigning variables men and women
        women = False
    elif('women' in 'preferences'):
        women =  preferences['men']
        men = False
    else:   #ideally shouldt come here
        men = False
        women  = False
        
        
    if(not(men and women)):
    #either men or women  
        if(men):
            gender = 'male'
        else:
            gender = 'female'
        for prospects in s:
            if(prospects.get('gender') == str(gender)):
                accept= advanced_filter(prospects, preferences, user_dict)
                if (accept):
                    filtered_list.append(prospects)
                    

    else: 
        for prospects in s:
            accept = advanced_filter(prospects, preferences, user_dict)
            if(accept):
                filtered_list.append(prospects)
                   

    return filtered_list


# In[371]:


def advanced_filter(prospects,preferences,user_dict):
    """
    This is an advanced filter funtion, that checks email id format. total number of friends
    this also checks if the prospecs preference criteria is met by the user
    """
    
    #installed_state = 0
    fbid = prospects.get('facebookId')
    accept = True
    #age wise filter-----------------------------------
    ageMin = (preferences.get('ageMin'))
    if ageMin == None:
        ageMin = -1                 #Random number because user has no lower limit pref
    ageMax = (preferences.get('ageMax'))
    if ageMax == None:
        ageMax = 500                #Random number because user has no upper limit pref
    if ('age' in prospects):
        if((prospects['age'] < ageMin) or (prospects['age'] > ageMax)):
            accept = False
            return accept
    else:         # as age is a compulsory field
        accept = False
        return accept
    #---------------------------------------------------- 
    #Userinterest
    if (('userInterests' in prospects) == False):
        accept = False
        return accept
           
    #Preferences must have atleast one gender variable
    if(('preferences' in prospects)):
        ## Rember to work on AgeMax and AgeMin
        if(( ('men' in prospects['preferences']) or ('women' in prospects['preferences']) ) == False):
                
            accept = False
            return accept
        if(prospects['preferences'].get('matchMaker') == True):
            accept = False
            return accept
           
     #Blocked------------------- ---  
    if(prospects.get('blocked') == True):
        accept = False
        return accept
     #--------------------------------
           
     #Deactivated check -------------------      
    if(prospects.get('deActivated') == True):
        accept = False
        return accept
#----------------------------

  #Email ending check---------------------  
    if(ENV_VARIABLE == "production"):
        if('email' in prospects):
           if(prospects['email'].endswith('@tfbnw.net') == True): 
                accept = False
                return accept
 #--------------------------------------     

 #Friends below 10 Nos.--------
    if(ENV_VARIABLE == "production"):
        if('totalFriends' in prospects):
            if( int(prospects.get('totalFriends')) < 10 ):
                accept = False
                return accept
     #-----------------------------  

 #already linked filter
    union = set()
    
    if ('likes' in user_dict):
        union = set(user_dict['likes'])|union

    if ('rejects' in user_dict):
        union = set(user_dict['rejects'])|union

    #if ( 'matched' in user_dict):
        #union = set(user_dict['matched'])|union

    if ( 'reported' in user_dict):
        union = set(user_dict['reported'])|union

    if ( 'aiProspects' in user_dict): 
        union = set(user_dict['aiProspects'])|union

    if ( 'instiProspects' in user_dict):
        union = set(user_dict['instiProspects'])|union

    if ( 'interestProspects' in user_dict):
        union = set(user_dict['interestProspects'])|union

    if ( 'randomProspects' in user_dict):
        union = set(user_dict['randomProspects'])|union

    if ( 'prospectLevel3' in user_dict):
        union = set(user_dict['prospectLevel3'])|union

    if ( 'anyProspects' in user_dict):
        union = set(user_dict['anyProspects'])|union

 #---------------------------------------       
    if (fbid in union):
        accept = False
        return accept
 #------------------------------------------
 
 
 # matched --------------------------------
    if ( 'matched' in user_dict):
        for k in user_dict['matched']:
            if(fbid == k.get('charm') ):
                accept = False
                return accept
 #---------------------------------------------

 # connected-----------------------------

    if ('connected' in user_dict):
        for k in user_dict['connected']:
            if(fbid == k.get('charm') ):
                
                accept = False
                return accept
  #-----------------------------------------        

           
    return accept


# In[372]:


#called by findprospects()
#returns filtered dataFrame based on users interest
def query_next_batch(batch_no,preferences,fbusers,batch_size,user_dict):
    """
    To query a batch of size = batch_size
    """
    lim = batch_size
    skip = batch_no*batch_size 
    s = fbusers.find().sort('_id',1).skip(skip).limit(lim)
    user_batch = getFilteredUsers(s,preferences,user_dict)
    if(len(user_batch)!=0):
        df = getFilteredDataframe(user_batch)
    else:
        user_batch = []
        df = pd.DataFrame(user_batch)
    return  df, user_batch 


# In[373]:


#called by findprospects()
# This Function filters prospects based on whether the user given matches the prospect's preferences
def filter_based_on_prospects_pref(prospect_preferences_dict,user_json):
    user_dict = json.loads(user_json)
    
    match = True        #final bool return
    
    if(('men' in prospect_preferences_dict) and ('women' in prospect_preferences_dict)):
        men = prospect_preferences_dict['men']
        women = prospect_preferences_dict['women']
    elif('men' in prospect_preferences_dict):
        men =  prospect_preferences_dict['men']                                #gender preference of prospect
        women = False
    elif('women' in prospect_preferences_dict):
        women =  prospect_preferences_dict['men']
        men = False
    else:
        men = False
        women  = False
  
    ageMax= prospect_preferences_dict.get('ageMax')
    if ageMax == None:
        ageMax = 500     # random upper limit as prospect has no upper limit preference
    ageMin = prospect_preferences_dict.get('ageMin')
    if ageMin == None:
        ageMin = -1      # random lower limit as user doesnt care about lower limit
    
        if(men and women):      
            if(user_dict['age']>ageMax or user_dict['age']<ageMin):
                match = False
                return match

        elif(men or women):
            if(men):
                if((user_dict['age']>ageMax) or (user_dict['age']<ageMin) or (str(user_dict.get('gender')) == 'female')):
                    match = False
                    return match
                    #print('hey')

            elif(women):
                if((user_dict['age']>ageMax) or (user_dict['age']>=ageMin) or (str(user_dict.get('gender')) == 'male')):
                    #print('waddup')
                    match = False
                    return match
    return match  


# In[374]:


#called by findprospects()
#This Function filters out nullValues and empty userInterests 
def getFilteredDataframe(allUsers):
    """
    removes empty userInterests data oints
    """
    
    
    df = pd.DataFrame(allUsers)
    df_temp = df[df['userInterests'].notna()]
    df_temp1 = df_temp[df_temp.astype(str)['userInterests'] != '[]']
    df_temp2 = df_temp1[df_temp1.astype(str)['userInterests'] != '{}']
    df_filtered = df_temp2
    return df_filtered


# In[375]:


#user_JSON = '{"_id": {"$oid": "5b47a645213a0500044e0dfa"}, "email": "ftenc_oyjirvc_user@tfbnw.net", "zodiac": "Pisces", "birthday": "02/25/1995", "gender": "male", "displayName": "Ftenc", "token": "EAADO6dNZBibgBAP3XM4qDMDFzfbvJtGZCw5r1t5XvpUnEgVdE6gIqC7fCGM8OUb2W8Pe0aHZAAMdFOcc1L3NW253GAiYjOVWPOKWZAtSV2nBmhnVAkMmSHmLEC4DiA61OFHhEs6mqcZAEG5SOAA9U5uuYJ7BZAAQOYIelZCqyqk2PojRtAlsJP8", "tempAccessToken": "EAADO6dNZBibgBAFJGuFytiO4hZAOT4iGj8mKwnyAEeYlGS3J6iVneJRG2QAelDVSQv9QG8VbZA7LkCZCfu4VCEddVuLkjsHxcUWAqryPVylHDLsANHSFaErOjrKDHfxO5HPrwDfcCvRI7GZB9qZCGLr6QBNdh7UNXcBYqOzWtKyRZB1z3KZAwcnVqURrCJ1UWWxZA2mVolQpey40UIIxNaVqO", "facebookId": "148356799322819", "invitees": ["112483639545557"], "prospectPoints": [], "age": 25, "maxProspects": 1, "cache": [], "updatedAt": {"$date": 1585936186962}, "createdAt": {"$date": 1531422277387}, "wingProspects": [], "prospects": [], "abuseCount": 1, "blocked": false, "interests": {"books": [], "films": [], "music": [], "others": [], "tvProg": []}, "preferences": {"distance": 25, "ageMin": 18, "ageMax": 36, "men": true, "women": true, "alerts": true, "matchMaker": false, "language": "en", "phone": {}}, "reportedBy": [{"reason": "Spam Profile", "fbId": "148356799322819", "_id": {"$oid": "5b4f523db5fd3a0004334f65"}}], "reported": ["148356799322819"], "matched": [], "connected": [{"charm": "145066602984780", "approver": "147504569407762", "_id": {"$oid": "5b47a85c213a0500044e0e0e"}}, {"_id": {"$oid": "5cefaca44ba6ee00043c84b3"}, "approver": "168515450552571", "charm": "168515450552571"}, {"_id": {"$oid": "5de0088caab9de00042498c6"}, "approver": "148356799322819", "charm": "118319695665621"}], "flames": 680, "rejects": ["160190401472214", "145066602984780", "10156522212389018"], "likes": ["145066602984780", "160190401472214", "137014393795770", "172695863468574", "168515450552571", "156531601838751", "118319695665621"], "friends": ["147504569407762", "156531601838751", "153707575458746", "123517251811434", "141753023322033", "118319695665621", "103770670468182"], "profilePics": ["148356799322819/0"], "work": [{"employer": "zomato", "position": ""}], "education": [{"school": "PES University"}], "__v": 26, "firebaseToken": "cvxu1ZaGlow:APA91bGm2tT-x0jdX7bvfKTOoBDMUPW1-zV3b0q9Y5b3biZNE_SuSkPPqy6LTw4BPEDmvm7WjtddC26kxtzyMYFJZJINHfCTq0SrHadZVeWoRaV9xQ2S2RWx9dnNrbWDuxeSaz7FmcuC", "imageUrl": "148356799322819/thumbnail", "lastProspCalc": {"$date": 1573648740000}, "activeUser": true, "lastWingProspCalc": {"$date": 1567511748530}, "installed": true, "leaderPoints": 780, "invitedBy": "1619813554831830", "uninstallDate": {"$date": 1578332346097}, "profileComplete": 84, "androidVer": "1.4.44", "lastSeen": {"$date": 1586352314689}, "iosVer": "2.6", "deviceName": "Xiaomi Redmi 4", "deActivated": false, "unsubscribed": false, "userInterests": [{"playlist": ["World Music", "Country"]}, {"vacation": ["Wildlife enthusiast", "Road tripper"]}, {"party": ["Wine sipper"]}, {"food": ["Low on calories"]}, {"fitness": ["Diet control"]}, {"sports": ["Basketball"]}, {"personality": ["GoGetter"]}, {"art": ["Dance Machine"]}, {"books": ["Read occasionally"]}, {"movie": ["Romantic"]}, {"animals": ["Aquatic Animals"]}, {"timepass": ["Latest Trends"]}, {"politics": ["Communism"]}], "bounce": [], "about": "about me", "totalFriends": 7, "aiProspects": [], "anyProspects": [], "instiProspects": [], "interestProspects": [], "prospectLevel3": [], "randomProspects": []}'


# In[376]:



user_dict = loads(user_json)
user_pref = user_dict['preferences']

found = 0
flag = 0
batch_size = batch_size
iters = (count//batch_size)
output_list = []
output_list_taste = []


for i in range(iters+1):
    #print(iters)
    #if i ==9:
        #print(found)
    if(user_dict.get('userInterests') == None):
        break
    elif(user_dict['userInterests'] == []):
        break
    if(found >= number_of_prospects):
        flag = 1
        break
    df_filtered_batch,batch_users = query_next_batch(i,user_pref,fbusers,batch_size,user_dict)
    if (len(batch_users)==0):
        continue
    ##(df_filtered_batch)
    #print(batch_users)
    df_filtered_with_prospect_pref = df_filtered_batch[df_filtered_batch['preferences'].apply(lambda x:filter_based_on_prospects_pref(x,user_json))]

    if (len(df_filtered_with_prospect_pref) ==0):
        continue
    #df_filtered_with_prospect_pref
    #print('hey')
    like_list = list_of_liked_users(user_dict)# gets list of users liked by the user
    #print('hey')
    
    mean_taste_dict = find_mean_interest_score(like_list) # calculates mean interest
    
    #print(mean_taste_dict)
    
    final_list = getFinalList(user_json= user_json, allUsers= batch_users, df_filtered= df_filtered_with_prospect_pref,mean_taste_dict = mean_taste_dict)
    #return final_list

    if len(final_list) == 0:
        continue

    final_df = pd.DataFrame(final_list)

    columns_order = ['facebookId', 'interest_score']

    final_reordered_interest = final_df[['facebookId', 'interest_score']]
    
    final_reordered_taste = final_df[['facebookId', 'taste_score']]
    
    final_reordered_thresholded_taste = final_reordered_taste[final_reordered_taste['taste_score']>taste_threshold_score]
     
    #print(final_reordered_thresholded_taste)
    final_reordered_thresholded = final_reordered_interest[final_reordered_interest['interest_score']>threshold_score]
    
    if len(final_reordered_thresholded_taste)>0:
        
        final_reordered_thresholded_taste = final_reordered_thresholded_taste.reindex(columns = ['facebookId', 'taste_score'])
        needed_taste_prospects = number_of_taste_prospects - len(output_list_taste)
        
        if(len(final_reordered_thresholded_taste) <= needed_taste_prospects):
        #found = found + len(final_reordered_thresholded_taste)
            for k in range(len(final_reordered_thresholded_taste)):
                    output_list_taste.append(final_reordered_thresholded_taste.iloc[k:k+1,:].values.tolist())
        
        else:
            #found = found + needed_prospects
            for k in range(needed_taste_prospects):
                    output_list_taste.append(final_reordered_thresholded_taste.iloc[k:k+1,:].values.tolist())
                    
                    
    if len(final_reordered_thresholded) == 0:
        continue
    #if i==9:
                #print(found)                                                 
    final_reordered_thresholded = final_reordered_thresholded.reindex(columns = columns_order)

    needed_prospects = number_of_prospects - len(output_list)
    #print(needed_prospects)
    #if(i ==9):
        #print(len(output_list))
    if(len(final_reordered_thresholded) <= needed_prospects):
        found = found + len(final_reordered_thresholded)
        for j in range(len(final_reordered_thresholded)):
                output_list.append(final_reordered_thresholded.iloc[j:j+1,:].values.tolist())

    else:
        found = found + needed_prospects
        for j in range(needed_prospects):
                output_list.append(final_reordered_thresholded.iloc[j:j+1,:].values.tolist())       
    #if i==9:
sorted_output_list = sorted(output_list,key = lambda x:x[0][1],reverse = True)

sorted_output_list_taste =  sorted(output_list_taste,key = lambda x:x[0][1],reverse = True)

client.close()
# Remember to remove the connection from other function
#print(set(sorted_output_list) - set(sorted_output_list_taste))


for i in (sorted_output_list_taste):
    dic ={}
    dic['facebookId'] = i[0][0]
    dic['taste_score'] = i[0][1]
    json = dumps(dic)
    sys.stdout.write(json)
    sys.stdout.write(str('\n'))
#print(sorted_output_list_taste)

for i in (sorted_output_list):
    #if i not in sorted_output_list_taste:
        dic ={}
        dic['facebookId'] = i[0][0]
        dic['interest_score'] = i[0][1]
        json = dumps(dic)
        sys.stdout.write(json)
        sys.stdout.write(str('\n'))
    
    
        


# In[377]:



#Example usage
#findProspects(user_fbid, total number of documents in collection, batch size of query )

# In[23]:




