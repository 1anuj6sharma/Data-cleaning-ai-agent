import pandas as pd
from dotenv import load_dotenv
import os
import json

from langchain_groq import ChatGroq
from langgraph.graph import StateGraph, END
from pydantic import BaseModel

# ================= LOAD ENV =================
load_dotenv()
groq_api_key = os.getenv("GROQ_API_KEY")

if not groq_api_key:
    raise ValueError("❌ GROQ_API_KEY is missing. Set it in .env file")

# ================= MODEL =================
llm = ChatGroq(
    groq_api_key=groq_api_key,
    model_name="openai/gpt-oss-20b",
    temperature=0
)

# ================= STATE =================
class CleaningState(BaseModel):
    input_text: str
    structured_response: str = ""

# ================= AGENT =================
class AIAgent:

    def __init__(self):
        self.graph = self.create_graph()

    def create_graph(self):
        graph = StateGraph(CleaningState)

        def agent_logic(state: CleaningState) -> CleaningState:
            response = llm.invoke(state.input_text)

            return CleaningState(
                input_text=state.input_text,
                structured_response=response.content
            )

        graph.add_node("cleaning_agent", agent_logic)
        graph.add_edge("cleaning_agent", END)
        graph.set_entry_point("cleaning_agent")

        return graph.compile()

    # ================= PROCESS DATA =================
    def process_data(self, df, batch_size=20):
        cleaned_data = []

        for i in range(0, len(df), batch_size):
            df_batch = df.iloc[i:i + batch_size]

            prompt = f"""
You are an AI Data Cleaning Agent.

Dataset:
{df_batch.to_string(index=False)}

Tasks:
- Handle missing values
- Remove duplicates
- Fix formats

⚠️ IMPORTANT:
Return ONLY valid JSON.
NO explanation.
NO text.

Format:
[
  {{"col1": value, "col2": value}},
  {{"col1": value, "col2": value}}
]
"""

            state = CleaningState(input_text=prompt, structured_response="")
            response = self.graph.invoke(state)

            if isinstance(response, dict):
                response = CleaningState(**response)

            raw_output = response.structured_response.strip()

            # 🔥 CLEAN JSON RESPONSE
            try:
                # Remove unwanted text if AI adds extra
                json_start = raw_output.find("[")
                json_end = raw_output.rfind("]") + 1
                clean_json = raw_output[json_start:json_end]

                parsed = json.loads(clean_json)

                if isinstance(parsed, list):
                    cleaned_data.extend(parsed)

            except Exception:
                # fallback: skip bad batch
                continue

        # 🔥 FINAL RETURN AS DATAFRAME
        if cleaned_data:
            return pd.DataFrame(cleaned_data)

        return df  # fallback
