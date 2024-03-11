# gk-for-u😎
###########################################################################################################
YouTube Data-Harvesting and Ware-Housing Using SQL, Data-Management Using Mongo and Using Streamlit to Create Web-App
###########################################################################################################
## Introduction 

* YouTube, the online video-sharing platform, has revolutionized the way we consume and interact with media. Launched in 2005, it has grown into a global phenomenon, serving as a hub for entertainment, education, and community engagement. With its vast user base and diverse content library, YouTube has become a powerful tool for individuals, creators, and businesses to share their stories, express themselves, and connect with audiences worldwide.

* This project extracts data from a particular youtube channel using the youtube channel id, processes the data, and stores it in the MongoDB database. It has the option to migrate the data to MySQL from MongoDB then analyse the data and give the results depending on the customer questions.
### 1. Tools Install
* Virtual Studio code.
* Python 3.11.8 or higher.
* MySQL.
* MongoDB.
  
* ### 2. Requirement Libraries to Install
* pip install google-api-python-client, pymongo, mysql-connector-python, pandas, streamlit.

### 3. Import Libraries
**Youtube API libraries**
* import googleapiclient.discovery
* from googleapiclient.discovery import build

**MongoDB**
* import pymongo

**SQL libraries**
* import mysql.connector

**pandas, numpy**
* import pandas as pd

**Dashboard libraries**
* import streamlit as st

### 4. E T L Process

a) **Extract data**

* Extract the particular youtube channel data by using the youtube channel id, with the help of the youtube API developer console.

b) **Process and Transform the data**

* After the extraction process, takes the required details from the extraction data and transform it into JSON format.

c) **Load  data** 

* After the transformation process, the JSON format data is stored in the MongoDB database, also It has the option to migrate the data to MySQL database from the MongoDB database.

### 5. E D A Process and Framework

a) **Access MySQL DB** 

* Create a connection to the MySQL server and access the specified MySQL DataBase by using pymysql library and access tables.

b) **Filter the data**

* Filter and process the collected data from the tables depending on the given requirements by using SQL queries and transform the processed data into a DataFrame format.

c) **Visualization** 

* Finally, select a question from the drop down menu to analyse the data and show the output in Dataframe Table.


## User Guide
**1.Paste your API_KEY(generate from google developer console) and the Youtube Channel_id on the sidebar input box.
**2.In the center you have a selectbox containing following functions.
    **1.**Channel Detail** - select this option to fetch the general details(channel name,channel id,channel subscriber count,total videos uploaded,playlist id) of the channel.
    **2.**Video Details** - select this option to fetch the all the video details(video name, video description, video likes,video comment,video views,video duration).
    **3.**Comment Details** - select this option to fetch all the comment details(comment, comment id, comment author, comment likes) of all the videos in the channel.
    **4.**Migrate Data to MongoDB** - select this option to store the data fetched from Youtube to Mongo database.
    **5.**Migrate Data From MongoDB to MySql** - select this option to fetch the entire data(all collection\document) in Mongo Database to store it in MySql Database.
    **6.**List of Channels Stored in Mongo** - select this option to view the name of channels that is stored in Mongo database.
    **7.**Store Data in Mysql Using Channel Name** - select this option to fetch data from Mongo database using the channel name and store it in MySql database.
    **8.**Queries** - Upon selecting this option it will show a new drop down box which has multiple question. select those question and the data will be fetched accordingly from the 
                      MySql database.

