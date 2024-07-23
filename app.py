import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get Snowflake credentials from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Verify that environment variables are loaded
if not all([SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA]):
    st.error("Missing one or more environment variables for Snowflake connection.")
    st.stop()

# Snowflake connection details
connection_parameters = {
    "account": SNOWFLAKE_ACCOUNT,
    "user": SNOWFLAKE_USER,
    "password": SNOWFLAKE_PASSWORD,
    "role": SNOWFLAKE_ROLE,
    "warehouse": SNOWFLAKE_WAREHOUSE,
    "database": SNOWFLAKE_DATABASE,
    "schema": SNOWFLAKE_SCHEMA
}

# Create a Snowflake session
try:
    session = Session.builder.configs(connection_parameters).create()
except Exception as e:
    st.error(f"Failed to create Snowflake session: {e}")
    st.stop()

# Original Streamlit app logic
pd.set_option("max_colwidth", None)
num_chunks = 3

def create_prompt(myquestion, rag):
    if rag == 1:
        cmd = """
         with results as
         (SELECT RELATIVE_PATH,
           VECTOR_COSINE_SIMILARITY(docs_chunks_table.chunk_vec,
                    SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', ?)) as similarity,
           chunk
         from docs_chunks_table
         order by similarity desc
         limit ?)
         select chunk, relative_path from results 
         """
        df_context = session.sql(cmd, params=[myquestion, num_chunks]).to_pandas()
        context_length = len(df_context) - 1

        prompt_context = ""
        for i in range(context_length):
            prompt_context += df_context._get_value(i, 'CHUNK')

        prompt_context = prompt_context.replace("'", "")
        relative_path = df_context._get_value(0, 'RELATIVE_PATH')

        prompt = f"""
          'You are an expert assistance extracting information from context provided. 
           Answer the question based on the context. Be concise and do not hallucinate. 
           If you don’t have the information just say so.
          Context: {prompt_context}
          Question:  
           {myquestion} 
           Answer: '
           """
        cmd2 = f"select GET_PRESIGNED_URL(@docs, '{relative_path}', 360) as URL_LINK from directory(@docs)"
        df_url_link = session.sql(cmd2).to_pandas()
        url_link = df_url_link._get_value(0, 'URL_LINK')

    else:
        prompt = f"""
         'Question:  
           {myquestion} 
           Answer: '
           """
        url_link = "None"
        relative_path = "None"

    return prompt, url_link, relative_path

def complete(myquestion, model_name, rag=1):
    prompt, url_link, relative_path = create_prompt(myquestion, rag)
    cmd = f"""
             select SNOWFLAKE.CORTEX.COMPLETE(?,?) as response
           """
    df_response = session.sql(cmd, params=[model_name, prompt]).collect()
    return df_response, url_link, relative_path

def display_response(question, model, rag=0):
    response, url_link, relative_path = complete(question, model, rag)
    res_text = response[0].RESPONSE
    st.markdown(res_text)
    if rag == 1:
        display_url = f"Link to [{relative_path}]({url_link}) that may be useful"
        st.markdown(display_url)

# Main code
st.title("Asking Questions to Your Own Documents with Snowflake Cortex:")
st.write("""You can ask questions and decide if you want to use your documents for context or allow the model to create their own response.""")
st.write("This is the list of documents you already have:")
docs_available = session.sql("ls @docs").collect()
list_docs = [doc["name"] for doc in docs_available]
st.dataframe(list_docs)

# Here you can choose what LLM to use. Please note that they will have different cost & performance
model = st.sidebar.selectbox('Select your model:',(
                                    'mixtral-8x7b',
                                    'snowflake-arctic',
                                    'mistral-large',
                                    'llama3-8b',
                                    'llama3-70b',
                                    'reka-flash',
                                     'mistral-7b',
                                     'llama2-70b-chat',
                                     'gemma-7b'))

question = st.text_input("Enter question", placeholder="Is there any special lubricant to be used with the premium bike?", label_visibility="collapsed")

rag = st.sidebar.checkbox('Use your own documents as context?')

if rag:
    use_rag = 1
else:
    use_rag = 0

if question:
    display_response(question, model, use_rag)
