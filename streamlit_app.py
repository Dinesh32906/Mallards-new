import streamlit as st
from snowflake.connector import connect
import pandas as pd
from config import get_snowflake_connection  # Correct import statement

# Default values
num_chunks = 3
slide_window = 7
model_name = 'mixtral-8x7b'  # Fixed model

def init_session_state():
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "use_chat_history" not in st.session_state:
        st.session_state.use_chat_history = True

def init_messages():
    if "clear_conversation" in st.session_state and st.session_state.clear_conversation:
        st.session_state.messages = []

def get_similar_chunks(question, session):
    cmd = """
        WITH results AS (
            SELECT RELATIVE_PATH,
                   VECTOR_COSINE_SIMILARITY(docs_chunks_table.chunk_vec,
                                            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', %s)) AS similarity,
                   chunk
            FROM docs_chunks_table
            ORDER BY similarity DESC
            LIMIT %s)
        SELECT chunk, relative_path FROM results
    """
    cur = session.cursor()
    cur.execute(cmd, (question, num_chunks))
    rows = cur.fetchall()
    cur.close()
    
    df_chunks = pd.DataFrame(rows, columns=['chunk', 'relative_path'])
    
    df_chunks_length = len(df_chunks) - 1

    similar_chunks = ""
    for i in range(df_chunks_length):
        similar_chunks += df_chunks.iloc[i]['chunk']
    similar_chunks = similar_chunks.replace("'", "")
    
    return similar_chunks

def get_chat_history():
    chat_history = []
    start_index = max(0, len(st.session_state.messages) - slide_window)
    for i in range(start_index, len(st.session_state.messages)):
        chat_history.append(st.session_state.messages[i]["content"])
    return chat_history

def summarize_question_with_history(chat_history, question, session):
    prompt = f"""
        Based on the chat history below and the question, generate a query that extends the question
        with the chat history provided. The query should be in natural language. 
        Answer with only the query. Do not add any explanation.
        
        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
        """
    
    cmd = """
        SELECT snowflake.cortex.complete(%s, %s) AS response
    """
    cur = session.cursor()
    cur.execute(cmd, (model_name, prompt))
    rows = cur.fetchall()
    cur.close()
    
    summary = rows[0][0]
    summary = summary.replace("'", "")
    return summary

def create_prompt(myquestion, session):
    if st.session_state.use_chat_history:
        chat_history = get_chat_history()
        if chat_history:
            question_summary = summarize_question_with_history(chat_history, myquestion, session)
            prompt_context = get_similar_chunks(question_summary, session)
        else:
            prompt_context = get_similar_chunks(myquestion, session)
    else:
        prompt_context = get_similar_chunks(myquestion, session)
        chat_history = ""

    prompt = f"""
        You are an expert chat assistant that extracts information from the CONTEXT provided
        between <context> and </context> tags.
        You offer a chat experience considering the information included in the CHAT HISTORY
        provided between <chat_history> and </chat_history> tags.
        When answering the question contained between <question> and </question> tags
        be concise and do not hallucinate. 
        If you don’t have the information just say so.
        
        Do not mention the CONTEXT used in your answer.
        Do not mention the CHAT HISTORY used in your answer.
        
        <chat_history>
        {chat_history}
        </chat_history>
        <context>
        {prompt_context}
        </context>
        <question>
        {myquestion}
        </question>
        Answer:
        """
    return prompt

def complete(myquestion, session):
    prompt = create_prompt(myquestion, session)
    cmd = """
        SELECT snowflake.cortex.complete(%s, %s) AS response
    """
    cur = session.cursor()
    cur.execute(cmd, (model_name, prompt))
    rows = cur.fetchall()
    cur.close()
    
    return rows[0][0]

def main():
    init_session_state()
    
    # Display the logo
    st.image("logo.png", use_column_width=True)  # Adjust the path if needed

    st.title("💬 Mallards AI Assistance")
    st.write("Ask your questions about Mallards below:")
    
    conn = get_snowflake_connection()
    if conn:
        session = conn
        init_messages()

        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        if question := st.chat_input("What do you want to know about Mallards?"):
            st.session_state.messages.append({"role": "user", "content": question})
            with st.chat_message("user"):
                st.markdown(question)
            with st.chat_message("assistant"):
                message_placeholder = st.empty()
                question = question.replace("'", "")
                with st.spinner(f"{model_name} thinking..."):
                    response = complete(question, session)
                    res_text = response
                    res_text = res_text.replace("'", "")
                    message_placeholder.markdown(res_text)
            st.session_state.messages.append({"role": "assistant", "content": res_text})
        session.close()
    else:
        st.error("Failed to connect to Snowflake.")

if __name__ == "__main__":
    main()
