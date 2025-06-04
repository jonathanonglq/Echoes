import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from datetime import date, timedelta
import json
import re
import boto3
from streamlit_extras.let_it_rain import rain

st.set_page_config(layout="wide")

s3 = boto3.client(
    "s3",
    aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    region_name=st.secrets["AWS_DEFAULT_REGION"]
)
    
def easter_egg(emoji):
    rain(
        emoji=emoji,
        font_size=54,
        falling_speed=5,
        animation_length="infinite",
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

def remove_invalid_unicode(message):
    if not isinstance(message, str):
        return message
    try:
        return message.encode('utf-16','surrogatepass').decode('utf-16')
    except:
        return message.encode('utf-8','ignore').decode('utf-8')
    
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
    df_temp['content'] = df_temp['content'].apply(decode_message)

    temp_key = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].startswith("X")]
    obj = s3.get_object(Bucket=st.secrets["BUCKET_NAME"], Key=temp_key[0])
    json_temp = json.loads(obj['Body'].read())['messages']

    df_temp2 = pd.json_normalize(json_temp)[["senderName","timestamp","text"]]
    df_temp2["timestamp"] = pd.to_datetime(df_temp2['timestamp'],unit='ms')
    df_temp2['text'] = df_temp2['text'].apply(remove_invalid_unicode)

    df_temp2.columns = df_temp.columns

    df = pd.concat([df_temp, df_temp2]).sort_values(by = 'timestamp_ms', ascending = False).reset_index(drop = True)

    df['timestamp_ms'] = df['timestamp_ms'] + timedelta(hours = 8)
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

    df = cached_load_data()
    # df = load_data()

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
    df['time_group'] = df['timestamp_ms'].dt.to_period('M').astype(str)

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

    filter_mode = st.radio("Select how you want to filter messages:", ["Keyword & Date", "Message Index"])

    df_view = df[['timestamp_ms', 'sender_name', 'content', 'word_count']].copy()
    df_view.columns = ['Timestamp', 'Sender', 'Message', 'Word Count']

    if filter_mode == "Keyword & Date":

        filter_input = st.text_input("Filter messages containing word(s):", placeholder="e.g. eat, pray, love")

        min_date = df['timestamp_ms'].min().date()
        max_date = df['timestamp_ms'].max().date()

        col1, col2 = st.columns(2)
        start_date = col1.date_input("Start Date:", value=min_date, min_value=min_date, max_value=max_date)
        end_date = col2.date_input("End Date:", value=max_date, min_value=min_date, max_value=max_date)

        date_mask = (df_view['Timestamp'].dt.date >= start_date) & (df_view['Timestamp'].dt.date <= end_date)

        if filter_input.strip():
            keywords = [w.strip().lower() for w in filter_input.split(',')]
            keyword_mask = df_view['Message'].fillna("").str.lower().apply(lambda text: any(word in text for word in keywords))
        else:
            keyword_mask = True

        combined_mask = date_mask & keyword_mask
        filtered_df = df_view[combined_mask]

        if (start_date == date(2020, 1, 19) and end_date == date(2024, 11, 26) and filter_input.strip().lower() == "love"):
            easter_egg("❤️")
            
        st.dataframe(filtered_df, use_container_width=True)

    elif filter_mode == "Message Index":

        col3, col4, col5 = st.columns(3)
        
        main_index = col3.number_input("Main Index", min_value=0, max_value=len(df_view)-1, value=0, step=1)
        n_before = col4.number_input("Number of Messages Before", min_value=0, max_value=main_index, value=0, step=1)
        n_after = col5.number_input("Number of Messages After", min_value=0, max_value=len(df_view)-1-main_index, value=0, step=1)

        start_idx = max(main_index - n_before, 0)
        end_idx = min(main_index + n_after + 1, len(df_view))

        filtered_df = df_view.iloc[start_idx:end_idx]

        st.dataframe(filtered_df, use_container_width=True)