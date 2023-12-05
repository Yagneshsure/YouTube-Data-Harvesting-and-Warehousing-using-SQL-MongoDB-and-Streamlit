#################################### Importing required Libaries  ##################################
import os
import pandas as pd
import pymongo

# import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors


##################################  Api Connection ##################################

def Api_connect():
    Api_Id="AIzaSyCmshDKVc4x0MSbb9MnzGAraYXPJ6y-Dcs"
    
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=Api_Id)
    
    return youtube

youtube = Api_connect()


############################### Channel Information #########################################

# Get Channel Information

def get_channel_info(channel_id):
    request = youtube.channels().list(
                    part = "snippet, ContentDetails, statistics",
                    id = channel_id
    )


    response = request.execute()

    for i in response['items']:
        data = dict(Channel_Name = i["snippet"]["title"],
                    Channel_id = i["id"],
                    Total_Subscriber = i["statistics"]["subscriberCount"],
                    Total_Views= i["statistics"]["viewCount"],
                    Channel_Description = i["snippet"]["description"],
                    Playlist_id = i["contentDetails"]["relatedPlaylists"]["uploads"]
                )
    return data



######################################### Video Ids ############################################

# Getting video ids

def get_videos_ids(channel_id):
    video_ids = []

    response = youtube.channels().list(id = channel_id,
                                        part = 'contentDetails').execute()
    Playlist_Id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
                                                part = 'snippet',
                                                playlistId = Playlist_Id,
                                                maxResults = 50,
                                                pageToken = next_page_token
                                            ).execute()
    # Playlist_Id

        for i in range(len(response1['items'])):
            video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token = response1.get('nextPageToken')
        
        if next_page_token is None:
            break  
    return video_ids


##############################################  Video Information ##################################################

# Getting video Information

def get_video_info(Video_Ids):

    video_data = [] 

    for video_id in video_Ids:
        request = youtube.videos().list(
            part = "snippet, contentDetails, statistics",
            id = video_id
        )
        response = request.execute()
        
        for item in response["items"]:
            data = dict(
                Channel_Name = item['snippet']['channelTitle'],
                Channel_Id  = item['snippet']['channelId'],
                Video_Id = item['id'],
                Video_Titel = item['snippet']['title'],
                Video_Tags = item.get('tags'),
                Video_Description = item['snippet']['description'],
                Video_thumbnail = item['snippet']['thumbnails'],
                Video_Published_Date = item['snippet']['publishedAt'],
                Video_Views = item['statistics']['viewCount'],
                Video_Likes = item['statistics']['likeCount'],
                video_Comments = item['statistics']['commentCount'],
                Favorite_Count = item['statistics']['favoriteCount'],
                Video_PlayTime = item['contentDetails']['duration'],
                Video_Definition = item['contentDetails']['definition'],
                Caption_Status = item['contentDetails']['caption'],
                Licensed_Content = item['contentDetails']['licensedContent']
                )
            
            video_data.append(data)
        
    return video_data

##################################################### Getting Comment Information ###################################################

# Getting Comment Information

def get_comment_info(video_ids):
    
    
    Comment_Data = []
    
    try:
        for video_id in video_ids:
            
            request = youtube.commentThreads().list(
                part="snippet",
                videoId = video_id,  # Correct parameter name
                maxResults=50
            )
            
            response = request.execute()
            
            for item in response['items']:
                data = dict(
                    Comment_Id = item['snippet']['topLevelComment']['id'],
                    Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                    Channel_Id = item['snippet']['topLevelComment']['snippet']['channelId'],
                    Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published_Date = item['snippet']['topLevelComment']['snippet']['publishedAt']
                    )
                
                Comment_Data.append(data)
            
    except:
        pass
    
    return Comment_Data



################################### Getting Playlist Details #################################################

def get_playlist_info(channel_id):

    next_page_token = None
    Playlist_Data = []
    
    while True:
        try:
            request = youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            
            for item in response['items']:
                data = dict(
                    PlayList_Id=item['id'],
                    PlayList_Title=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    Playlist_Published_Date=item['snippet']['publishedAt'],
                    Playlist_Video_Count=item['contentDetails']['itemCount']
                )
                
                Playlist_Data.append(data)
            # data
            
        except Exception as e:
            print(f"An error occurred: {e}")
            break
        
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
        
    return Playlist_Data


######################################################### Mongo DB ############################################

# Connecting to Mongodb
client = pymongo.MongoClient("mongodb+srv://yagneshsure10:9591083438@cluster0.fvyb8ub.mongodb.net/?retryWrites=true&w=majority")

db = client["Youtube_data"] # Creating a new database named youtube


# Uploding data to mongodb

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = get_videos_ids(channel_id)
    vi_info = get_video_info(vi_ids)
    cm_info = get_comment_info(vi_ids)
    
    coll1 = db["channel_details"]
    coll1.insert_one({"Channel_information":ch_details, "Playlist_information":pl_details, "Video_Ids" : vi_ids, 
                            ":Video_information":vi_info, "Comment_information" : cm_info })
    
    return "Upload completed successfully"


################################################## 