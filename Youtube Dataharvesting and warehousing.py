import googleapiclient.discovery
import streamlit as st
import pandas as pd
import mysql.connector
import re
from pymongo import MongoClient

# to connect with youtube api server
def Youtube_connect(keys):
    key = keys
    youtube = googleapiclient.discovery.build("youtube", 
                                                "v3", 
                                                developerKey= key 
                                                )
    return youtube
#to get channel info
def channel_info(channel_id,youtube):
    request = youtube.channels().list(part="snippet,contentDetails,statistics",
                                      id=channel_id
                                      )
    response = request.execute()
    Channel_information={
    'channel_id': channel_id,
    'Channel_Name':response['items'][0]['snippet']['title'],
    'Channel_Description':response['items'][0]['snippet']['description'],
    'Channel_startDate':response['items'][0]['snippet']['publishedAt'],
    'Playlist_id':response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
    'Channel_Subscriber':int(response['items'][0]['statistics']['subscriberCount']),
    'Total_Videos':int(response['items'][0]['statistics']['videoCount']),
    }
    
    return Channel_information
#to get video id's of entire videos in channel
def video_Id(channel_id,youtube):
    pageToken1= None
    data=[]
    request = youtube.channels().list(part="snippet,contentDetails,statistics",
                                      id=channel_id
                                      )
    response = request.execute()
    id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    while True:
        request = youtube.playlistItems().list(part="snippet,contentDetails",
                                           playlistId=id,
                                           maxResults=50,
                                           pageToken= pageToken1)
        response = request.execute()
   
        for i in range(len(response['items'])):
            y=response['items'][i]['contentDetails']['videoId']
            data.append(y)
        if 'nextPageToken' not in response:
            break
        pageToken1=response['nextPageToken'] 
    return data  
#to get video details by using video id's 
def video_info( Video_id, youtube,channel_id):
        z=[]
        for i in range(len(Video_id)):
                request = youtube.videos().list(
                                 part="snippet,contentDetails,statistics",
                                 id = Video_id[i]
                                           )
                response = request.execute()
                data1={
                         'channel_id':channel_id,
                         'channel_name':response['items'][0]['snippet']['channelTitle'],
                         'video_title':response['items'][0]['snippet']['title'],
                         'video_description':response['items'][0]['snippet']['description'],
                         'video_id':response['items'][0]['id'],
                         'video_publishedAt':response['items'][0]['snippet']['publishedAt'],
                         'video_tag':response['items'][0]['snippet'].get('tags','NAn'),
                         'video_duration':response['items'][0]['contentDetails']['duration'],
                         'video_views':int(response['items'][0]['statistics']['viewCount']),
                         'video_likes':int(response['items'][0]['statistics'].get('likeCount','0')),
                         'video_CommentCounts':int(response['items'][0]['statistics'].get('commentCount','0'))
                             
                             }
                z.append(data1)
        return z
#to get comments details from video id's
def Comment_Info(Video_id, youtube,channel_id):
    m = []
    for video_id in Video_id:
        pagetoken1 = None
        try:
            while True:
                request = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=video_id,
                    maxResults=100,
                    pageToken=pagetoken1
                )
                response = request.execute()

                pagetoken1 = response.get('nextPageToken')

                for i in range(len(response['items'])):
                    data = {
                        'channel_id':channel_id,
                        'video_id': video_id,
                        'comments_id': response['items'][i]['id'],
                        'comment_Author': response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'comment': response['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal'],
                        'comment_Likes': int(response['items'][i]['snippet']['topLevelComment']['snippet'].get('likeCount',0)),
                        'Comment_PublishedAt': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
                    m.append(data)

                if 'nextPageToken' not in response:
                    break

        except Exception as e:
            u = {video_id: "Comments_Disabled"}
            m.append(u)

    return m  
#connection of sql
def sql_connection():
     sqldb = mysql.connector.connect(host="localhost", user="root", password ="012345", database="giri")
     cursor = sqldb.cursor()
     return cursor,sqldb
#connection to mongo db
def mongo_connection():
     mongodb = MongoClient("localhost",27017)
     db = mongodb['Youtube']
     collection = db['Channels_Doc']
     return collection
# mongo db data insertion
def mongoinsertion(m):
    try:
         collection = mongo_connection()
    except:
         return "Sorry Server Error"
    try:
       collection.insert_one(m) 
       return "Successfully uploaded in Mongodb"
    except:
        return "Sorry for the delay: Mongo Insertion Problem"   
#sql channel table creation and data insertion
def sql_channel_upload(cursor,sqldb,d):
    #channel details
    exception=[]
    success=[]
    #conditions to create tables    
    df = pd.DataFrame(d)
    query= """create table if not exists channel_details( 
                                                channel_id varchar(60) primary key,
                                                Channel_Name varchar(60),
                                                Channel_Description text,
                                                Channel_startDate varchar(60),
                                                Playlist_id varchar(60),
                                                Channel_Subscriber int,
                                                Total_Videos int
                                                
                                                ) """
    cursor.execute(query)
    #sql channel data insertion

    for i,row in df.iterrows(): 
            try:
                    insert = """insert into channel_details(  
                            channel_id,
                            Channel_Name,
                            Channel_Description,
                            Channel_startDate,
                            Playlist_id,
                            Channel_Subscriber,
                            Total_Videos
                            )
                                    
                            values(%s,%s,%s,%s,%s,%s,%s)"""

                    values = (row['channel_id'],
                                    row['Channel_Name'],
                                    row['Channel_Description'],
                                    row['Channel_startDate'],
                                    row['Playlist_id'],
                                    row['Channel_Subscriber'],
                                    row['Total_Videos']
                                    )
                    cursor.execute(insert,values)
                    sqldb.commit()
                    success.append("Done")
            except Exception as e:
                    exception.append(e)
    if success:
            st.success(f"{len(success)} Channel data Successfully uploaded to MySql")
    if exception:
            st.error(f"Duplicate Channel entries found, {len(exception)} uploads skipped")
    return "Success"
#sql video table creation and data insertion
def sql_video_upload(cursor,sqldb,d):
    #video details
    exception=[]
    success=[]
    #converting data into dataframe and concatnating
    u=[]
    for i in range(len(d)):
        v= pd.DataFrame(d[i])
        u.append(v)
    s= pd.concat(u, ignore_index= True)
    #video duration conversion
    hours = minutes = seconds = 0
    for i, row in s.iterrows():
        match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', row['video_duration'])
        if match:
            hours = int(match.group(1) or 0)
            minutes = int(match.group(2) or 0)
            seconds = int(match.group(3) or 0)
            s.at[i, 'video_duration'] = f'{hours:02d}:{minutes:02d}:{seconds:02d}'
    #sql video table creation 

    query= """create table if not exists video_details( 
                                    channel_id varchar(60), 
                                    channel_name varchar(100),
                                    video_title varchar(200),
                                    video_description text,
                                    video_id varchar(60) primary key,
                                    video_publishedAt varchar(50),
                                    video_tag text,
                                    video_duration varchar(20),
                                    video_views int,
                                    video_likes int,
                                    video_CommentCounts int,
                                    foreign key (channel_id) references channel_details(channel_id)
                                    ) """

    cursor.execute(query)
    #sql video data insertion

    for i,row in s.iterrows(): 
        try:
            insert = """insert into video_details(  
                                    channel_id,
                                    channel_name,
                                    video_title,
                                    video_description,
                                    video_id,
                                    video_publishedAt,
                                    video_tag,
                                    video_duration,
                                    video_views,
                                    video_likes,
                                    video_CommentCounts 
                                    )
                                        
                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            video_tag_str = ','.join(row['video_tag'])

            values = (  row['channel_id'],
                        row['channel_name'],
                        row['video_title'],
                        row['video_description'],
                        row['video_id'],
                        row['video_publishedAt'],
                        video_tag_str,
                        row['video_duration'],
                        row['video_views'],
                        row['video_likes'],
                        row['video_CommentCounts']
                        )
            cursor.execute(insert,values)
            sqldb.commit()
            success.append("Done")
        except Exception as e:
            exception.append(e)
    if success:
        st.success(f"{len(success)} Video data Successfully uploaded to MySql")
    if exception:
        st.error(f"Duplicate Video entries found,{len(exception)} uploads skipped")
    return "Success"
#sql comment table creation and data insertion
def sql_comments_upload(cursor,sqldb,d):
    #comment details
    success=[]
    exception=[]

    try:
        z=[]
        for i in range(len(d)):
            df= pd.DataFrame(d[i])
            z.append(df)
        c= pd.concat(z,ignore_index=True)
    except:
         c = pd.DataFrame(d)
    #to drop unnecessary columns
    col = ['channel_id','video_id','comments_id','comment_Author','comment','comment_Likes','Comment_PublishedAt']
    for i in c.columns:
        if i not in col:
            c.drop(columns=[i], inplace=True)
    #to fill none values with 0
    c.fillna(0, inplace=True)
    #to create comment table in sql
    query= """create table if not exists comment_details( 
                                                channel_id varchar(60),
                                                video_id varchar(60),
                                                comments_id varchar(200) primary key,
                                                comment_Author varchar(60),
                                                comment text,
                                                comment_Likes int,
                                                Comment_PublishedAt varchar(60),
                                                foreign key (channel_id) references channel_details(channel_id),
                                                foreign key (video_id) references video_details(video_id)
                                                ) """

    cursor.execute(query)

    #sql comment data insertion
    for i,row in c.iterrows(): 
        try:
            insert = """insert into comment_details(  
                                            channel_id,
                                            video_id,
                                            comments_id,
                                            comment_Author,
                                            comment,
                                            comment_Likes,
                                            Comment_PublishedAt
                                            )
                                                    
                                            values(%s,%s,%s,%s,%s,%s,%s)"""

        
            values = (row['channel_id'],
                    row['video_id'],
                    row['comments_id'],
                    row['comment_Author'],
                    row['comment'],
                    row['comment_Likes'],
                    row['Comment_PublishedAt']
                    )
            cursor.execute(insert,values)
            sqldb.commit()
            success.append("Done")
        except Exception as e:
            exception.append(e)
    if success:
        st.success(f"{len(success)} comment data Successfully uploaded to MySql")
    if exception:
        st.error(f"Duplicate comment entries found,{len(exception)} uploads skipped")
    return "Success"
#to migrate entire data from mongo db to sql
def sql_insertion(collection,cursor,sqldb):
    keys = ["Channel_Details","Video_Details","Comment_Details"]
    for key in keys:
        #data collection from mongodb for the given key
        d=[]
        for data in collection.find({},{"_id":0,key:1}):
               d.append(data[key])
       #conditions to create tables
        if key == "Channel_Details":
            g = sql_channel_upload(cursor,sqldb,d) 
            d=[]    
        elif key == "Video_Details":
             h = sql_video_upload(cursor,sqldb,d)
             d=[]
        elif key == "Comment_Details":
             b = sql_comments_upload(cursor,sqldb,d)
             d=[]
    if g and h and b == "Success":
        cursor.close()
        sqldb.close()        
        return "Successfully Upload Completed"

#integrated streamlit code for web app interface
#code for app sidebar
st.sidebar.title(":rainbow[YouTube Data Harvesting And Warehousing]")
st.sidebar.header(":red[Overview]",divider='rainbow')
st.sidebar.caption(
            """
            :gray[Aim is to develop a user-friendly application 
            that utilizes the Google API to extract information 
            on a YouTube channel, stores it in a MongoDB database, 
            migrates it to a SQL data warehouse, and enables users 
            to analyse channel details for meaningful insights.]"""
            )    

keys = st.sidebar.text_input("Paste Your API Key")
channel_id = st.sidebar.text_input("Enter the Channelid you want to scrape")
if keys and channel_id:
     s = Youtube_connect(keys)
   
#streamlit main page  
st.write("Let's get started")

if 'Channel' not in st.session_state:
     st.session_state['Channel'] = ""
if 'Video_info' not in st.session_state:
     st.session_state['Video_info'] = ""
if 'comments' not in st.session_state:
     st.session_state['comments'] = ""

operation0 = st.selectbox("Select a Operation",["None",
                                                "Channel Details",
                                                "Video Details",
                                                "Comment Details",
                                                "Migrate Data to MongoDB",
                                                "Migrate Data From MongoDB to MySql", 
                                                "List of Channels Stored in Mongo", 
                                                "Store Data in Mysql Using Channel Name",
                                                "Queries"])
if operation0 == "Migrate Data to MongoDB":
    if st.session_state['Channel'] and st.session_state['Video_info'] and st.session_state['comments']:
        details ={"Channel_Details":st.session_state['Channel'],"Video_Details":st.session_state['Video_info'],"Comment_Details":st.session_state['comments']}
        y= mongoinsertion(details)
        st.success(y)
    else:
        youtube = Youtube_connect(keys)
        Channel = channel_info(channel_id,youtube)
        st.session_state['Channel'] = Channel
        Video_id = video_Id(channel_id,youtube)
        Video_info = video_info(Video_id,youtube,channel_id)
        st.session_state['Video_info'] = Video_info
        comments = Comment_Info(Video_id,youtube,channel_id)
        st.session_state['comments'] = comments
        details ={"Channel_Details":Channel,"Video_Details":Video_info,"Comment_Details":comments}
        y= mongoinsertion(details)
        st.success(y)
elif operation0 == "Migrate Data From MongoDB to MySql":
     s = mongo_connection()
     m,t = sql_connection()
     y = sql_insertion(s,m,t)
     st.success(y)
elif operation0 == "List of Channels Stored in Mongo":
     s = mongo_connection()
     d=[]
     for data in s.find({},{"_id":0,"Channel_Details":1}):
        d.append(data["Channel_Details"]["Channel_Name"])
     y = {"Channel Names":d}
     st.table(y)     
elif operation0 == "Channel Details":
        s= Youtube_connect(keys)
        t= channel_info(channel_id,s)
        st.session_state['Channel'] = t
        st.table({"Channel Details" :t})        
elif operation0 == "Video Details":
        s= Youtube_connect(keys)
        t= video_Id(channel_id,s)
        m = video_info(t,s,channel_id)
        st.session_state['Video_info'] = m
        st.success(f"Details of {len(m)} Videos Extracted")
        st.dataframe(m)
elif operation0 == "Comment Details":
        s= Youtube_connect(keys)
        t= video_Id(channel_id,s)
        m= Comment_Info(t,s,channel_id)
        st.session_state['comments'] = m
        st.success(f"{len(m)} Comment Details Extracted")
        st.dataframe(m)
elif operation0 == "Store Data in Mysql Using Channel Name":
        u = st.text_input("Paste the channel Name ")
        if u:
            #checking whether the data exist in mysql or not
            m,t = sql_connection()
            s = mongo_connection()
            d= []
            for i in s.find({},{"_id":0}):
                if i["Channel_Details"]["Channel_Name"] == u:
                    d.append(i)
            query = "select * from channel_details where channel_name = %s "
            m.execute(query,[u])
            y = m.fetchall()
            if y:
                st.success("Channel Details Already exists in MySql Database")
            else:
                st.error("Channel name not found in MySQL, Extracting from MongoDB and Uploading...")
                g = sql_channel_upload(m,t,[d[0]["Channel_Details"]])
            #video details check and upload
            query = "select * from video_details where channel_name = %s"
            m.execute(query,[u])
            v = m.fetchall()
            if v:
                st.success("Video Details Already exists in Mysql Database")
            else:
                q = sql_video_upload(m,t,[d[0]["Video_Details"]])
            #comment details check and upload
            query = "select channel_id from channel_details where Channel_Name = %s"
            m.execute(query,[u])
            y= m.fetchall()
            query = "select * from comment_details where channel_id = %s"
            m.execute(query,y[0])
            z= m.fetchall()
            if z:
                st.success("Comment Details Already exists in Mysql Database")
            else:
                c = sql_comments_upload(m,t,d[0]["Comment_Details"])

            m.close()
            t.close()
elif operation0 == "Queries":
     operation1= st.selectbox("Please Select Your Question",["None",
                                         "What are the names of all the videos and their corresponding channels?",
                                         "Which channels have the most number of videos, and how many videos do they have?",
                                         "What are the top 10 most viewed videos and their respective channels?",
                                         "How many comments were made on each video, and what are their corresponding video names?",
                                         "Which videos have the highest number of likes, and what are their corresponding channel names?",
                                         "What is the total number of likes for each video, and what are their corresponding video names?",
                                         "What is the total number of views for each channel, and what are their corresponding channel names?",
                                         "What are the names of all the channels that have published videos in the year 2022?",
                                         "Which videos have the highest number of comments, and what are their corresponding channel names?",
                                         "What is the average duration of all videos in each channel and what are their corresponding channel names?" 
                                         ]
                                         )
     m,t = sql_connection()
     if operation1 == "What are the names of all the videos and their corresponding channels?":
        query = "select channel_name,video_title from video_details"
        m.execute(query)
        y =m.fetchall()
        df= pd.DataFrame(y,columns=["Channel Name","Video Name"])
        st.dataframe(df)
     elif operation1 == "Which channels have the most number of videos, and how many videos do they have?":
        query = "select channel_name,Total_Videos from channel_details order by Total_Videos desc"
        m.execute(query)
        y =m.fetchall()
        df= pd.DataFrame(y,columns=["Channel Name","Total_Video"])
        st.dataframe(df)
     elif operation1 == "What are the top 10 most viewed videos and their respective channels?":
        query = "select channel_name,video_title,video_views from video_details order by video_views desc limit 10"
        m.execute(query)
        y = m.fetchall()
        df= pd.DataFrame(y,columns=["Channel Name","Video Name","Video Views"])
        st.dataframe(df)
     elif operation1 == "How many comments were made on each video, and what are their corresponding video names?":
        query = "select channel_name,video_title,video_CommentCounts from video_details order by video_CommentCounts desc"
        m.execute(query)
        y = m.fetchall()
        df= pd.DataFrame(y,columns=["Channel Name","Video Name","Total Comments"])
        st.dataframe(df)
     elif operation1 == "Which videos have the highest number of likes, and what are their corresponding channel names?":
        query = "select channel_name,video_title,video_likes from video_details order by video_likes desc limit 10"
        m.execute(query)
        y = m.fetchall()
        df= pd.DataFrame(y,columns=["Channel Name","Video Name","Total Likes"])
        st.dataframe(df)
     elif operation1 == "What is the total number of likes for each video, and what are their corresponding video names?":
        query = "select video_title,video_likes from video_details "
        m.execute(query)
        y = m.fetchall()
        df= pd.DataFrame(y,columns=["Video Name","Total Likes"])
        st.dataframe(df)
     elif operation1 == "What is the total number of views for each channel, and what are their corresponding channel names?":
        query = "select channel_name,sum(video_views) from video_details group by channel_name "
        m.execute(query)
        y = m.fetchall()
        df= pd.DataFrame(y,columns=["Chanel Name","Total Views"])
        st.dataframe(df)
     elif operation1 == "What are the names of all the channels that have published videos in the year 2022?":
        query = "select channel_name from video_details where year(video_publishedAt) = 2022 group by channel_name"
        m.execute(query)
        y = m.fetchall()
        df= pd.DataFrame(y,columns=["Chanel Name"])
        st.dataframe(df)
     elif operation1 ==  "Which videos have the highest number of comments, and what are their corresponding channel names?":
        query = "select channel_name, video_title,video_CommentCounts from video_details order by video_CommentCounts desc limit 10"
        m.execute(query)
        y = m.fetchall()
        df= pd.DataFrame(y,columns=["Chanel Name","Video Name","Total Comment"])
        st.dataframe(df)
     elif operation1 == "What is the average duration of all videos in each channel and what are their corresponding channel names?":
        query = "select channel_name, SEC_TO_TIME(AVG(TIME_TO_SEC(video_duration))) from video_details11 group by channel_name"
        m.execute(query)
        y = m.fetchall()
        df = pd.DataFrame(y,columns=["Channel Name","Avg Watchtime"])
        st.write(df)
    
    
     
                     


                 
                 
                   

        
            
            
          
     
     








