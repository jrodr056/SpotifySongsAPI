from tempfile import TemporaryFile
import pandas as pd
from pendulum import yesterday
from psycopg2 import connect
import requests
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import json
from datetime import datetime
import datetime
import sqlite3
import mysql.connector
TOKEN = ''
def validateData(df:pd.DataFrame)->bool:
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
    if not pd.Series(df['played_at']).is_unique:
        raise Exception("Primary Key Check is violated")
    
    if df.isnull().values.any():
        raise Exception("Null value in field")

    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0,minute=0,second=0,microsecond=0)
    timestamps = df['timestamp'].tolist()
    print(timestamps)
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp,"%Y-%m-%d") != yesterday:
            raise Exception("At least one song does not come from within last 24 hours")
    return True

def storeData(df:pd.DataFrame):
    connection = mysql.connector.connect(user="",password="",host="",database="")
    engine = sqlalchemy.create_engine("mysql+mysqlconnector://user:password"+"@localhost/test")
    cursor = connection.cursor()
    sqlCommand = """
    CREATE TABLE IF NOT EXISTS myPlayedTracks(song_name VARCHAR(200),
    artist_name VARCHAR(200),
    played_at VARCHAR(200),
    timestamp VARCHAR(200),
    CONSTRAINT primary_key_contstraint PRIMARY KEY(played_at)
    )"""
    cursor.execute(sqlCommand)
   
    df.to_sql(name="myPlayedTracks",con=engine,index=False,if_exists='append')
    
    # print("data already exists")
    cursor.close()

if __name__ == "__main__":
    headers = {
        "Accept":"application/json",
        "Content-Type":"application/json",
        "Authorization":"Bearer {token}".format(token=TOKEN),
    }
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterdayUnixTimestamp = int(yesterday.timestamp()) * 1000
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterdayUnixTimestamp),headers=headers)
    data = r.json()
    print(data) 
    songNames = []
    artistNames = []
    playedAtList = []
    timestamps = []

    for song in data['items']:
        songNames.append(song['track']['name'])
        artistNames.append(song['track']['album']['artists'][0]['name'])
        playedAtList.append(song['played_at'])
        timestamps.append(song['played_at'][0:10])

    songDict = {
        "song_name":songNames,
        "artist_name":artistNames,
        "played_at":playedAtList,
        "timestamp":timestamps
    }
    song_df = pd.DataFrame(songDict,columns=['song_name','artist_name','played_at','timestamp'])
    print(song_df)
    # if validateData(song_df):
        # print("Valid data, proceed to store")
    storeData(song_df)