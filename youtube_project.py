from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


#API key connection

def Api_connect():
    Api_Id="AIzaSyBMZmzwTFf4oM35Ypa0-q1sTfa-t90AiF0"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

#get channels information
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

#get video ids
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

#get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)    
    return video_data


#get comment information
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data

#get_playlist_details

def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Video_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data

#upload to mongoDB

client=pymongo.MongoClient("mongodb+srv://kumarappan:kumar123@cluster0.b1vcktc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["Youtube_data"]

def channel_details(channel_id):
  ch_details=get_channel_info(channel_id)
  pl_details=get_playlist_details(channel_id)
  vi_ids=get_videos_ids(channel_id)
  vi_details=get_video_info(vi_ids)
  com_details=get_comment_info(vi_ids)

  coll1=db["channel_details"]
  coll1.insert_one({"channel_information":ch_details,
                    "playlist_information":pl_details,
                    "video_information":vi_details,
                    "comment_information":com_details})

  return "upload successful"

# Table Creation
# Channels table
def channels_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="kumar123",
        database="youtube_data",
        port="5432"
    )

    cursor = mydb.cursor()

    #Drop table if exists
    drop_query = '''DROP TABLE IF EXISTS channels'''
    cursor.execute(drop_query)
    mydb.commit()

    # Create table
    create_query = '''CREATE TABLE IF NOT EXISTS channels (
                        Channel_Name VARCHAR(100),
                        Channel_Id VARCHAR(100) PRIMARY KEY,
                        Subscribers BIGINT,
                        Views BIGINT,
                        Total_Videos INT,
                        Channel_Description TEXT,
                        Playlist_Id VARCHAR(100)
                    )'''
    cursor.execute(create_query)
    mydb.commit()

    ch_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for ch_data in ch_list:
        insert_query = '''INSERT INTO channels (Channel_Name, 
                                                Channel_Id, 
                                                Subscribers, 
                                                Views, 
                                                Total_Videos, 
                                                Channel_Description, 
                                                Playlist_Id)

                                                
                        VALUES (%s, %s, %s, %s, %s, %s, %s)'''
        values = (ch_data['Channel_Name'], 
                ch_data['Channel_Id'], 
                ch_data['Subscribers'],
                ch_data['Views'], 
                ch_data['Total_Videos'], 
                ch_data['Channel_Description'], 
                ch_data['Playlist_Id'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except psycopg2.Error as e:
            print('Error inserting data:', e)

#Playlist table

def playlist_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="kumar123",
        database="youtube_data",
        port="5432"
    )

    cursor = mydb.cursor()

    #Drop table if exists
    drop_query = '''DROP TABLE IF EXISTS playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    # Create table
    create_query = '''CREATE TABLE IF NOT EXISTS playlist (Playlist_Id VARCHAR(100),
                                                            Title VARCHAR(100),
                                                            Channel_Id VARCHAR(100),
                                                            Channel_Name VARCHAR(100),
                                                            PublishedAt TIMESTAMP,
                                                            Video_Count int)'''
    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range (len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)

    for pl_data in pl_list:
            insert_query = '''INSERT INTO playlist (Playlist_Id,
                                                    Title,
                                                    Channel_Id,
                                                    Channel_Name,
                                                    PublishedAt,
                                                    Video_Count)
                            VALUES (%s, %s, %s, %s, %s, %s)'''
            
            values = (pl_data['Playlist_Id'], 
                    pl_data['Title'], 
                    pl_data['Channel_Id'],
                    pl_data['Channel_Name'], 
                    pl_data['PublishedAt'], 
                    pl_data['Video_Count'])
            try:
                cursor.execute(insert_query, values)
                mydb.commit()
            except psycopg2.Error as e:
                print('Error inserting data:', e)

#video table
def videos_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="kumar123",
        database="youtube_data",
        port="5432"
    )

    cursor = mydb.cursor()

    #Drop table if exists
    drop_query = '''DROP TABLE IF EXISTS videos'''
    cursor.execute(drop_query)
    mydb.commit()

    # Create table
    create_query = '''CREATE TABLE IF NOT EXISTS videos (Channel_Name VARCHAR(100),
                        Channel_Id VARCHAR(80),
                        Video_Id VARCHAR(50),
                        Title VARCHAR(150),
                        Tags TEXT,
                        Thumbnail VARCHAR(100),
                        Description TEXT,
                        Published_Date TIMESTAMP,
                        Duration INTERVAL,
                        Views BIGINT,
                        Likes BIGINT,
                        Comments INT,
                        Favorite_Count INT,
                        Definition VARCHAR(50),
                        Caption_Status VARCHAR(50))'''
        
    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range (len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)


    for vi_data in vi_list:
                insert_query = '''INSERT INTO videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Title,
                                                    Tags,
                                                    Thumbnail,
                                                    Description,
                                                    Published_Date,
                                                    Duration,
                                                    Views,
                                                    Likes,
                                                    Comments,
                                                    Favorite_Count,
                                                    Definition,
                                                    Caption_Status)
                                                    
                                VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s)'''
                
                values = (vi_data['Channel_Name'], 
                        vi_data['Channel_Id'], 
                        vi_data['Video_Id'],
                        vi_data['Title'], 
                        vi_data['Tags'], 
                        vi_data['Thumbnail'],
                        vi_data['Description'],
                        vi_data['Published_Date'], 
                        vi_data['Duration'], 
                        vi_data['Views'],
                        vi_data['Likes'], 
                        vi_data['Comments'], 
                        vi_data['Favorite_Count'],
                        vi_data['Definition'],
                        vi_data['Caption_Status'], 
                        
                        )
                try:
                    cursor.execute(insert_query, values)
                    mydb.commit()
                except psycopg2.Error as e:
                    print('Error inserting data:', e)



# Comments Table
def comments_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="kumar123",
        database="youtube_data",
        port="5432"
    )

    cursor = mydb.cursor()

    #Drop table if exists
    drop_query = '''DROP TABLE IF EXISTS comments'''
    cursor.execute(drop_query)
    mydb.commit()

    # Create table
    create_query = '''CREATE TABLE IF NOT EXISTS comments (Comment_Id VARCHAR(100) primary key,
                            Video_Id VARCHAR(100),
                            Comment_Text TEXT,
                            Comment_Author VARCHAR(50),
                            Comment_Published TIMESTAMP)'''
                        
    cursor.execute(create_query)
    mydb.commit()

    com_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range (len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)

    for com_data in com_list:
        insert_query = '''INSERT INTO comments(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Published)
                                                                        
                        VALUES (%s, %s, %s, %s, %s)'''
        
        values = (com_data['Comment_Id'], 
                com_data['Video_Id'], 
                com_data['Comment_Text'],
                com_data['Comment_Author'], 
                com_data['Comment_Published']
                
                )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except psycopg2.Error as e:
            print('Error inserting data:', e)

def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "Tables created successfully"

def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_playlists_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range (len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range (len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2

def show_comments_table():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range (len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3

#streamlit coding

with st.sidebar: 
    st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Requirements")
    st.caption("python scrypting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")

channel_id=st.text_input("Enter the Channel ID")

if st.button("Collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
       st.success("Channel Details of given channel already exists")
    
    else:
       insert=channel_details(channel_id)
       st.success(insert)

if st.button("Migrate to Sql"):
       Table=tables()
       st.success(Table)
    
show_table=st.radio("select the table for view",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()


elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()
   

#SQL Connection

mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="kumar123",
    database="youtube_data",
    port="5432"
)

cursor = mydb.cursor()

question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. Videos with higest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))

if question=="1. All the videos and the channel name":
    query1='''select title as videos, channel_name as channelname from videos''' #postgres all columns will be in lowercase
    cursor.execute(query1)
    mydb.commit()
    t1= cursor.fetchall()
    df=pd.DataFrame(t1,columns=["Video_title","Channel_Name"])
    st.write(df)

elif question=="2. channels with most number of videos":
    query2='''select channel_name as Channelname, total_videos as No_of_videos from channels
                order by total_videos desc''' #postgres all columns will be in lowercase
    cursor.execute(query2)
    mydb.commit()
    t2= cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No_of_videos"])
    st.write(df2)

elif question== "3. 10 most viewed videos":
    query3='''select views as views, channel_name as channelname, title as videotitle from videos 
            where views is not null order by views desc limit 10''' #postgres all columns will be in lowercase
    cursor.execute(query3)
    mydb.commit()
    t3= cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views", "channelname", "videotitle"])
    st.write(df3)

elif question== "4. comments in each videos":
    query4='''select comments as no_of_comments, title as videotitle from videos where comments is not null''' #postgres all columns will be in lowercase
    cursor.execute(query4)
    mydb.commit()
    t4= cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["No of comments", "videotitle"])
    st.write(df4)

elif question==  "5. Videos with higest likes":
    query5='''select title as videotitle, channel_name as channelname, likes as no_of_likes from videos
        where likes is not null order by likes desc '''
    cursor.execute(query5)
    mydb.commit()
    t5= cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle", "channelname","no_of_likes"])
    st.write(df5)

elif question==  "6. likes of all videos":
    query6='''select likes as likescount, title as videotitle from videos '''
    cursor.execute(query6)
    mydb.commit()
    t6= cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likescount", "videotitle"])
    st.write(df6)

elif question== "7. views of each channel":
    query7='''select views as viewscount, channel_name as channelname from channels '''
    cursor.execute(query7)
    mydb.commit()
    t7= cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["viewscount", "channelname"])
    st.write(df7)

elif question== "8. videos published in the year of 2022":
    query8='''select title as videotitle, published_date as publishdate, channel_name as channelname from videos 
    where extract (year from published_date)=2022  '''
    cursor.execute(query8)
    mydb.commit()
    t8= cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","publishdate" ,"channelname"])
    st.write(df8)

elif question== "9. average duration of all videos in each channel":
    query9='''select  channel_name as channelname, AVG(duration)as averageduration from videos group by channel_name '''
    cursor.execute(query9)
    mydb.commit()
    t9= cursor.fetchall()
    df9=pd.DataFrame(t9,columns=['channelname','averageduration'])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration= row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle =channel_title, avgduration=average_duration_str ))
    df01=pd.DataFrame(T9)
    st.write(df01)

elif question=="10. videos with highest number of comments":
    query10='''select  comments as no_of_comments, title as videotitle, channel_name as channelname from videos 
    where comments is not null order by comments desc '''
    cursor.execute(query10)
    mydb.commit()
    t10= cursor.fetchall()
    df10=pd.DataFrame(t10,columns=['no_of_comments','videotitle', 'channelname'])
    st.write(df10)


