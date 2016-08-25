
# coding: utf-8

# # P3: Wrangle OpenStreetMap Data
# 
# 
# This project extracted and cleaned OpenStreetMap data(https://en.wikipedia.org/wiki/OpenStreetMap) and stored it into MongoDB so that users can explore quality data fast.
# OpenStreetMap is map information in a topological data structure collected by volunteers. It consists of four major elements; Node, Ways, Relation and Tags. Nodes present geographic information of specified locations by points and create log such as user, update time etc. Ways represent linear features such as areas, rivers, streets and create log such as user, update time etc. Relations are representing relationship between Nodes and Ways, and Tags contains metadata of its Nodes or Ways elements.
# I used OpenStreetMap New York area of OSM file downloaded from MapZen (https://mapzen.com/data/metro-extracts/). Its size is about 737,545kb and it contains 3,449,666 elements consists of 2,961,075 'node's, 2571 'relation's, and 486020 'way' tags.
# Assessing and improving data quality, this project considered validity, accuracy, completeness, consistency, and uniformity of data. The whole process of project are as below:
# 
# 


### Auditing

import xml.etree.ElementTree as ET
import os
import re
from  collections import defaultdict
import pprint

from datetime import datetime

import json
from bson import json_util


# In[2]:



def add_to_dict(dic, dic_key, key_to_cnt):
    """Updating/initiating value for each attributes in the data."""
    
    updated_dic = dic
    try    : updated_dic[dic_key][key_to_cnt] +=1
    except : updated_dic[dic_key][key_to_cnt] = 1
    return updated_dic

def print_sorted_dict(d):
    keys = d.keys()
    keys = sorted(keys, key=lambda s: s.lower())
    for k in keys:
        v = d[k]
        print "%s: %d" % (k, v)
        
def stats_data(osm_file_name_input, pretty = False, tags = ('node', 'way', 'relation')):
    problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
    lower = re.compile(r'^([a-z]|_)*$')
    lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
    
    
    data=defaultdict(dict)
    errors=defaultdict(dict)
    
    stats = dict(tags_count = {}
                 , attrib_count={}
                 , tag_attrib_count={}
                 , non_chars_count ={}
                 , subgroups = {}
                )   
    
    context = iter(ET.iterparse(osm_file_name_input, events = ('start','end')))
    _, root = next(context)
    
    i=0
    for event, element in context:
        
        if event !='end' or element.tag not in tags:
            continue
        
        i+=1
        if i%100000 ==1:
            print i           
        
        element_dict={}   
        element_dict['tag_type'] = element.tag

        stats = add_to_dict(dic = stats, dic_key='tags_count', key_to_cnt=element.tag)
         ### exploring top level attributes
        for attr in element.attrib:
            key = attr
            val = element.attrib[key]            
            element_dict[key] = val         
            stats = add_to_dict(dic = stats, dic_key='attrib_count', key_to_cnt= key)
        
        id_ = element_dict['id']
        # print '1: ',element_dict
        
        #### exploring low level attributes
        for nested in element:
            try:
                key = nested.attrib['k']
                val = nested.attrib['v']
                if val=="{}": print "none ; ",val
                
            except KeyError:
                continue
            
            ## collecting keys and its values if the key is containing punctuations
            if key.startswith('cityracks'):
                key = ":".join(key.split('.'))
            
            if problemchars.search(key) :
                try:
                    stats['non_chars_count'][key].append(val)
                except KeyError:
                    stats['non_chars_count'][key]=[val]              
                    
            ## grouping attributes that have sub attributes
            
            elif len(key.split(':'))>=2:
                splited = key.split(':')
                group_key = splited[0]
                sub_group_key = ":".join(splited[1:])
                 
                if group_key not in element_dict.keys():
                    element_dict[group_key]= defaultdict(dict) 
                
                try:
                    element_dict[group_key][sub_group_key]=val
                except :
                    if key not in errors[element_dict['id']].keys():
                        errors[element_dict['id']]={key:[val]}
                    else :
                        errors[element_dict['id']][key].append(val)
                
                if group_key not in stats['subgroups'].keys():
                    stats['subgroups'][group_key]={sub_group_key:1}
                sub = stats['subgroups'][group_key]
                try :
                    sub[sub_group_key] +=1
                except KeyError:
                    sub[sub_group_key] =1

            else :
                element_dict[key] = val                
                stats = add_to_dict(dic = stats, dic_key='tag_attrib_count', key_to_cnt= key)
            
        
        data[id_] = element_dict
        
        root.clear()

        
    return data, stats, errors


# In[3]:

OSM_FILE_NAME = 'new_new-york_new-york'
OSM_FILE = os.path.join('data',OSM_FILE_NAME+".osm")
SAMPLE_FILE_NAME ='mini_sample_new-york_new-york' 
SAMPLE_FILE =os.path.join('data',SAMPLE_FILE_NAME+".osm")


# In[4]:

if False:
    osm_file_name =OSM_FILE_NAME
    osm_file= OSM_FILE


# In[5]:

if True:
    osm_file_name =SAMPLE_FILE_NAME
    osm_file= SAMPLE_FILE


# In[6]:

data, stats, errors  = stats_data(osm_file)


# In[7]:

pprint.pprint(stats)


# In[8]:

### creating a list of attributes to exclude: attributes that have less than 10 values.
tag_attrib_to_exclude=[key for key, val in stats['tag_attrib_count'].items() if val<=10]
print tag_attrib_to_exclude


# In[9]:

"""
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]
#added 'created_by' attribute of low level elements
CREATED.append('created_by')

"""
### street name : street type in expected
expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 'Turnpike',
            "Trail", "Parkway", "Commons",'Way','Bayside', 'Circle', 'Highway', 'West', 'South','East', 'North'
            ,'Plaza','Cove', 'Bowery', 'Concourse', 'Park', 'Terrace','Walk','Loop', 'Broadway', 'Oval', 'Crescent']

### pair of street types to fix and to correct street types
to_fix={'way':'Way', 'rd':'Road', 'avenue':'Avenue', 'st':'Street', 'ave':'Avenue', 'avene':'Avenue'
       , 'tpke':'Turnpike'}

# postcode starts with 0 or 1 in NY
postcode_type_re = re.compile('[0-9]{5}(?:-[0-9]{4})?$')


# In[10]:

def audit_timestamp(str_time):                              
    """
    Recording inappropriate date type in 'caution' dictionary and transforming timestamp in datetime object
    """
    try:
        time = datetime.strptime(str_time, "%Y-%m-%dT%H:%M:%SZ")
    except:
        cautions['timestamp'].append(str_time)
        time=str_time
    return time

def audit_street(street_name, id_):
    """
    Recording inappropriate street_type in 'caution' dictionary and returns it corrected.
    """
    
    street_type_re = re.compile(r'\S+\.?$', re.IGNORECASE)
    try: 
        # removing space start and end of the string and capitalize it.
        street_name = street_name.strip().capitalize()
        # take only street_type from the street_name
        street_type = street_type_re.search(street_name).group()
        # capitalize street type
        capitalized = street_type.capitalize()
    # collecting errors
    except : 
        street_type = "_"
        cautions['street'][street_type].append((id_,street_name))
        return street_name
    # transform data if its street type is listed in 'to_fix' dictionary.
    if street_type.lower() in to_fix.keys():
        to_return= street_name.replace(street_type,to_fix[capitalized.lower()])
    
    # collecting suspicious errors
    elif capitalized not in expected:
        to_return = street_name.replace(street_type,capitalized)
        cautions['street'][street_type].append((id_,street_name))
    
    else:
        to_return= street_name.replace(street_type,capitalized)
    
    return to_return


def audit_postcode(postcode, id_):
    """
    Recording inappropriate postcode data in 'caution' dictionary and returns it corrected.
    """
    
    postcode = postcode.strip()
    p = postcode_type_re.search(postcode)
    if p:
        to_return = p.group()
    else:    
        cautions['postcode'][id_]=postcode
        to_return=postcode
    return to_return


def audit_pos(lat,lon):
    """
    Recording inappropriate position data in 'caution' dictionary and returns it in a list of float.
    
    The valid range of latitude in degrees is -90 and +90 for the southern and northern hemisphere respectively.
        Longitude is in the range -180 and +180 specifying the east-west position.
    """

    
    try:
        lat = element['lat']
        lon = element['lon']
                
        lat = float(lat)
        lon = float(lon)

        if not -90<=lat<=90 or not -180<=lon<=180:
            cautions['pos'].append([lat, lon])

    except ValueError:
        cautions['pos'].append([lat, lon])
    
    return [lat,lon]


def audit_height(height,id_):
    """
    Recording inappropriate height data in 'caution' dictionary and returns it in float and in feet.
    """
    
    if isinstance(height, float):
        return height
    try :
        # remove foot symbol(') from the data
        if height.endswith("'"):
            height = float(height.replace("'",""))
            
        # remove inch symbol("") from the data
        elif height.endswith('"'):
            height = float(height.replace('"',''))/12
            
        height = float(height)

        if height<=4 and element['tag_type']=='node':
            cautions['height'][id_]=height
        else: heights.append(height)
    except ValueError:
        cautions['height'][id_]=element['height']
    
    return height


# In[11]:


def auditing_data(element):
    """
    Execute audit data of five attributes of element: street, postcode, timestamp, latitude, longtitude, and height 
    for an input element.
    """
    
    try:     id_ = element['id']
    except : cautions['id'].append(element)
    
    if 'addr' in element.keys():
        
        ## Street name auditing     
        try : 
            street_name = element['addr']['street']
            audit_street(street_name,id_)
        except : pass

        ## postcode
        try:
            postcode = element['addr']['postcode']
            if postcode:
                audit_postcode(postcode,id_)
        except:pass    
    
    ### timestamp
    try:
        str_time = element['timestamp']
        audit_timestamp(str_time)
    except: pass
    
    ### latitude and longtitude
    try:
        lat = element['lat']
        lon = element['lon']
        audit_pos(lat, lon)
    except: pass
    
    ### height: range, numeric?
    try:
        height = element['height']
        audit_height(height,id_)
    except : pass


# In[12]:

### Running Data Audit

# a collection for suspicious error data
cautions = dict(street=defaultdict(list)
               , postcode=defaultdict(dict)
               , timestamp=[]
               , pos=[]
               , height={}
               , id =[]) 

# stores values of heights to expore distribution of heights values
heights=[]

# executing audit for every element in the dataset.
for key, element in data.items():
    if element!={}:
        auditing_data(element)


# In[13]:

######################AUDITING RESULT ##############################################

### Checking Street data
st = cautions['street']

for key, val in st.items():
    print key
    print val


# In[14]:

#>> correct [('3580782407', 'West 80th Street NYC 10024')]: cut the postcode in the street field to postcode field
print data['3580782407']
data['3580782407']['addr']['postcode'] = '10024'
data['3580782407']['addr']['street'] = 'West 80th Street'
print
print data['3580782407']

#>> adding newly discovered street type in the 'expected' list.
expected.extend(['Path','Mall', 'Mews','Village','Slip', 'Ridge', 'Heights','Driveway', 'Roadbed', 'Center'
                 ,'Expressway','Rockaways'])

#>> add pairs to correct street type in the to_fix dictionary.
to_fix['aveneu'] = 'Avenue'
to_fix['pkwy'] = 'Parkway'
to_fix['dr.'] = 'Drive'
to_fix['ave.'] = 'Avenue'
to_fix['streeet'] = 'Street'
to_fix['st.'] = 'Street'
to_fix['blvd.']='Building'
to_fix['hwy'] = "Highway"
to_fix['blv.'] = 'Boulevard'
to_fix['dr'] = 'Drive'
to_fix['blvd'] = 'Boulevard'
to_fix['aveneu'] = 'Avenue'
to_fix['pl'] = 'Place'
to_fix['tirnpike']='Turnpike'
to_fix['ct']='Court'
to_fix['tunpike']='Turnpike'
to_fix['pky'] = 'Parkway'

print
print to_fix.keys()


# In[15]:

### checking postcode data
print cautions['postcode']


# In[16]:

#>> moving phone data into phone section from postcode
data['3810255154']['phone'] = cautions['postcode']['3810255154']
print data['3810255154']['phone']

#>> removing data which doesn't comform to postcode format.
for i in cautions['postcode'].keys():
    data[i]['addr']['postcode']=None


# In[17]:

### checking timestamp error data
print cautions['timestamp']


# In[18]:

### checking height error data

h = cautions['height']
    
print len(h), len(heights)

print max(heights)
print min(heights)

print h


# In[19]:

def audit_height(height,id_):
    """
    Modify 'audit_height' method to add regex for reshaping the height attribute in the same unit(feet).
    """

    inch = ['"', 'inch']
    feet = ['ft', "'", 'feet']
    meter = ['m', 'meter']
    
    if isinstance(height, float):
        return height
    
    try:
        ## inch
        for i in inch:
            if i in height:
                height = float(height.replace(i, "").strip())/12
                break
        ## meter

        for m in meter:
            if m in height:
                height = float(height.replace(m, "").strip()) * 3.28084
                break

        ## feet
        for f in feet:
            if f in height:
                height = float(height.replace(f, "").strip())
                break

        if not isinstance(height, float):
            height = float(height.strip())

        if height<=4 and element['tag_type']=='node':
            cautions['height'][id_]= height
        else: heights.append(height)

    except:            
        cautions['height'][id_] = height

    return height


# In[20]:

### checking error data latitude and longtitude

cautions['pos']


# ## Reshaping Data and Store Data In JSON File
# 
# Here, I reshaped the data and saved the data in JSON format to import it to the MongoDB.

# In[ ]:


# to save datetime object in json: referred> https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable-in-python


### processing data in an input osm file and save it in both JSON format and a list( 'data')

json_file_name_out = "%s.json" %(osm_file_name)

cautions = dict(street=defaultdict(list)
               , postcode=[]
               , id=[]
               , timestamp=[]
               , pos=[]
               , height={}) 

def osm_to_json(data, json_file_name_out):
    """
    Refine data from an original osm file and save it in both JSON format and a list( 'data')
    
    """
    problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
    CREATED = [ "version", "changeset", "timestamp", "user", "uid", 'created_by']
    
    with open(json_file_name_out,'w') as output:
        i=0
        for _id, element in data.items():
            ele = {'created':{}}

            for key, val in element.items():

                if key in tag_attrib_to_exclude or val == None:
                    continue

                if key== 'addr':
                    try:
                        addr = ele[key]
                    except KeyError:
                        ele[key]={}

                    for k, v in element[key].items():
                        if v=={} or v==None:
                            continue

                        if k =='postcode':
                            ele[key][k] = audit_postcode(v,_id)

                        elif k == 'street':
                            if v!='':
                                ele[key][k] = audit_street(v,_id)
                        else :
                            ele[key][k]=v              

                elif key == 'lat':
                    ele['pos'] = audit_pos(val, element['lon'])
                elif key =='height':
                    ele[key] = audit_height(val,_id)
                elif key in CREATED:
                    if key =='timestamp': val = audit_timestamp(val)
                    ele['created'][key]=val
                else:
                    ele[key] = val

            ### save the element dictionary in a JSON file
            output.write(json.dumps(ele, default = json_util.default)+'\n')

            i+=1
            if i%1000000==1: print i


# In[ ]:

osm_to_json(data, json_file_name_out)

### reviewing suspicious error data: street
for key, val in cautions['street'].items():
    print key
    pprint.pprint(val)


# In[ ]:

### reviewing suspicious error data: others
print cautions['postcode']
print cautions['timestamp']
print cautions['id']
print cautions['pos']
print cautions['height']


# # Loading and Exploring Data in MongoDB
# 
# In MongoDB Shell, one could import the data in JSON file created just before, typing command below.
# 
# "mongoimport --db p3(:db name) --collection new_york(:collection name) --file datafile.json"
# 

# In[32]:

### Connecting to my MongoDB db, 'p3'

from pymongo import MongoClient
client = MongoClient()

db = client.p3
collection = 'new_york'
#collection = 'sample_ny'


# ### CREATING INDEXES
# 
# Here created indexes for fields which are used for queries to improve speed performance. Especially, for 'pos' field, used GEO2D INDEX to make it possible to find near specified locations.

# In[ ]:

# Create a 2d Index: https://docs.mongodb.com/v3.0/tutorial/build-a-2d-index/
#### in pyMongo    : http://api.mongodb.com/python/1.7/examples/geo.html

if True:
    
    db[collection].create_index([('id',1)])
    db[collection].create_index([('timestamp',1)])
    db[collection].create_index([('name',1)])
    db[collection].create_index([('address',1)])
    
    from pymongo import GEO2D

    db[collection].create_index([('pos',GEO2D)])


# ## Executing Queries

# ### Query 0.1: number of unique users
# 

# In[78]:

len(db[collection].find(query).distinct('created.uid'))


# ### Query 0.2: number of amenities

# In[89]:

query ={'amenity':{'$ne': None}}
db[collection].find(query).count()


# ### Query 1:  Top 10 users who uploaded the data most often and frequency

# In[43]:

pipeline = [
    {'$group':{'_id': '$created.user', 'frequency': {'$sum':1} }}
    , {'$sort': {'frequency':-1}}    
    , {'$limit':10}    
    ]

result = db[collection].aggregate(pipeline)

for e in result:
    pprint.pprint(e)


# ### Query 2:  The number of data uploaded by year.

# In[34]:

### run query

# ref : https://docs.mongodb.com/v3.0/reference/operator/aggregation/month/
pipeline = [
            {'$project': {'year': { '$year': "$timestamp"}}}
             ,{'$group': {'_id':'$year', 'frequency': {'$sum': 1} }}
             ,{'$sort': {'_id':1}} 
        ]    

result = db[collection].aggregate(pipeline)

for r in result:
    pprint.pprint(r)


# ### Query 3:  Finding the Five Nearest locations from 'AJ's Pizza'.

# In[35]:

## looking for position of "AJ's Pizza".
name = "AJ's Pizza"
pipeline ={'$query': {'name':name}
        ,'$project':{'name':'$name', 'pos':'$pos'}
       }
result = db[collection].find(pipeline)

for r in result:
    
    position = r['pos']
    
print
print name,"'s location: ", position


# In[36]:

### Query a 2d Index: https://docs.mongodb.com/manual/tutorial/query-a-2d-index/

query = {'pos':{'$near':position}, 'name':{'$ne':None}, 'tag_type':'node', 'pos':{'$ne':position}}

result = db[collection].find(query).limit(5)

for doc in result:
    print
    pprint.pprint(doc)
    


# In[ ]:



