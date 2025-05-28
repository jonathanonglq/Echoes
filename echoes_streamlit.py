import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from datetime import date
import json
import re
import boto3

st.set_page_config(layout="wide")

s3 = boto3.client(
    "s3",
    aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    region_name=st.secrets["AWS_DEFAULT_REGION"]
)
    
def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    return [ atoi(c) for c in re.split(r'(\d+)', text) ]

def find_non_ascii(word):
    for char in word:
        if ord(char) > 127:
            return True
    return False

def decode_message(message):
    error_msg = '[decodingError]'
    try:
        words = message.split()
    except:
        return message
        
    for idx in range(len(words)):
        if find_non_ascii:
            try:
                words[idx] = words[idx].encode('latin1').decode('utf-8')
            except:
                words[idx] = error_msg

    return ' '.join(words)

def load_data():

    response = s3.list_objects_v2(Bucket=st.secrets["BUCKET_NAME"])
    json_keys = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].startswith("message")]

    json_files = []

    for key in json_keys:
        obj = s3.get_object(Bucket=st.secrets["BUCKET_NAME"], Key=key)
        raw_bytes = obj['Body'].read()
        json_files += json.loads(raw_bytes)['messages']

    df_temp = pd.json_normalize(json_files).iloc[:,:3]
    df_temp['timestamp_ms'] = pd.to_datetime(df_temp['timestamp_ms'],unit='ms')

    temp_key = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].startswith("X")]
    obj = s3.get_object(Bucket=st.secrets["BUCKET_NAME"], Key=temp_key[0])
    json_temp = json.loads(obj['Body'].read())['messages']

    df_temp2 = pd.json_normalize(json_temp)[["senderName","timestamp","text"]]
    df_temp2["timestamp"] = pd.to_datetime(df_temp2['timestamp'],unit='ms')
    df_temp2.columns = df_temp.columns

    df_temp['content'] = df_temp['content'].apply(lambda x: decode_message(x))
    df = pd.concat([df_temp, df_temp2]).sort_values(by = 'timestamp_ms', ascending = False).reset_index(drop = True)

    return df

@st.cache_data(show_spinner="Loading messages from AWS S3...", ttl=3600)
def cached_load_data():
    return load_data()

# --- Configuration ---
USERNAME = st.secrets["USERNAME"]
PASSWORD = st.secrets["PASSWORD"]
HER_NAME = st.secrets["HER_NAME"]
HIS_NAME = st.secrets["HIS_NAME"]

# --- Login State ---
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

# --- Login Interface ---
if not st.session_state.logged_in:
    st.title("🍋 Welcome to LimOngTea Conversations!")
    st.subheader("Please log in to continue")

    with st.form("login_form"):
        user = st.text_input("Username")
        pwd = st.text_input("Password", type="password")
        login_button = st.form_submit_button("Login")

        if login_button:
            if user == USERNAME and pwd == PASSWORD:
                st.session_state.logged_in = True
                st.success("✅ Logged in successfully!")
                st.rerun() 
            else:
                st.error("❌ Invalid credentials")
else:

    # df = cached_load_data()
    df = load_data()

    df['word_count'] = df['content'].str.split().str.len()
    df['word_count'] = df['word_count'].fillna(0)

    st.title("💞 Echoes")

    st.markdown("### ❤️ Our Love in Numbers")
    overview_kpi1, overview_kpi2 = st.columns(2)
    overview_kpi1.metric("Total Number of Messages", f"{len(df['word_count']):,.0f}")
    overview_kpi2.metric("Total Number of Words", f"{sum(df['word_count']):,.0f}")
    overview_kpi3, overview_kpi4 = st.columns(2)
    overview_kpi3.metric("Number of Messages by Her", f"{sum(df['sender_name'] == HER_NAME):,.0f}")
    overview_kpi4.metric("Number of Words by Her", f"{sum(df[df['sender_name'] == HER_NAME]['word_count']):,.0f}")
    overview_kpi5, overview_kpi6 = st.columns(2)
    overview_kpi5.metric("Number of Messages by Him", f"{sum(df['sender_name'] == HIS_NAME):,.0f}")
    overview_kpi6.metric("Number of Words by Him", f"{sum(df[df['sender_name'] == HIS_NAME]['word_count']):,.0f}")


    st.markdown("---")
    st.markdown("### 🌸 Seasons of Us")

    options = ["Day","Month","Year"]
    group_option = st.segmented_control("Group messages by:", options, default = "Month")

    # Derive time_group column based on selection
    if group_option == "Day":
        df['time_group'] = df['timestamp_ms'].dt.floor('D')
    elif group_option == "Month":
        df['time_group'] = df['timestamp_ms'].dt.to_period('M').astype(str)
    elif group_option == "Year":
        df['time_group'] = df['timestamp_ms'].dt.year.astype(str)

    # Count messages by sender and time_group
    msg_counts = df.groupby(['time_group', 'sender_name']).size().reset_index(name='message_count')

    # Altair stacked bar chart
    chart = alt.Chart(msg_counts).mark_bar().encode(
        x=alt.X('time_group:N', title=f'{group_option}'),
        y=alt.Y('message_count:Q', title='Number of Messages'),
        color=alt.Color('sender_name:N', title='Sender'),
        tooltip=['time_group', 'sender_name', 'message_count']
    ).properties(
        width=800,
        height=400,
        title=f'Messages per {group_option} by Sender'
    )

    st.altair_chart(chart, use_container_width=True)


    st.markdown("---")
    st.markdown("### 💬 Words That Matter")

    filter_input = st.text_input("Filter messages containing word(s):", placeholder="e.g. eat, pray, love")

    df_view = df[['timestamp_ms', 'sender_name', 'content', 'word_count']].copy()
    df_view.columns = ['Timestamp', 'Sender', 'Message', 'Word Count']

    if filter_input.strip():
        keywords = [w.strip().lower() for w in filter_input.split(',')]
        mask = df_view['Message'].fillna("").str.lower().apply(lambda text: any(word in text for word in keywords))
        filtered_df = df_view[mask]
    else:
        df_view = df_view.sort_values(by="Timestamp", ascending = False)
        filtered_df = df_view

    # Show filtered table
    st.dataframe(filtered_df, use_container_width=True)
