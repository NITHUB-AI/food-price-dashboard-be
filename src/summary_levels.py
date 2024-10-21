#Add OpenAI library
import openai
from openai import AzureOpenAI

import google.generativeai as genai
import os
from dotenv import load_dotenv
load_dotenv()


openai.api_key = os.getenv('AZURE_OPENAI_API_KEY')
openai.api_base =  os.getenv('ENDPOINT')
openai.api_type = 'azure' # Necessary for using the OpenAI library with Azure OpenAI
openai.api_version = '2023-05-15' # Latest / target version of the API

deployment_name = 'Voicetask' # SDK calls this "engine", but naming
                                   # it "deployment_name" for clarity
                      
client = AzureOpenAI(
    api_version=openai.api_version,
    azure_endpoint=openai.api_base,
    azure_deployment=deployment_name,
    api_key=openai.api_key
)

def summarize_gpt(news, model_name="gpt-3.5-turbo", deployment_name='Voicetask'):

    prompt = f"""
    Using all the news provided, generate a comprehensive summary highlighting the parts of the news most relevant to factors that could affect food prices (e.g insecurity, recession, pandemic, fuel scarcity, covid, corona, electricity, etc.).
    Exclude prayers and other generic statements. 
    Limit the summary to a maximum of 150 words in total.

    News: {news}
    """
    try:
        response = client.chat.completions.create(
            temperature=0.4,
            # engine=deployment_name,
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a great News Aggregator specialization in food security and factors affecting food prices."},
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message.content
    except:
        return ''
    

api_key=os.environ['GEMINI_API_KEY']

genai.configure(api_key=api_key)

# Create the model
generation_config = {
  "temperature": 1,
  "top_p": 0.95,
  "top_k": 40,
  "max_output_tokens": 8192,
  "response_mime_type": "text/plain",
}
 
def summarize_gemini(news,model='gemini-1.5-flash-8b'):
    
    model = genai.GenerativeModel(
    model_name="gemini-1.5-flash-8b",
    generation_config=generation_config,
                                )
    prompt = f"""
    Using all the news provided, generate a comprehensive summary highlighting the parts of the news most relevant to factors that could affect food prices (e.g insecurity, recession, pandemic, fuel scarcity, covid, corona, electricity, etc.).
    Pay attention to the dates the each summary was published. This should help provide more context for you when carrying out your analysis
    Exclude prayers and other generic statements. 
    Do not include information not included in the news.
    Limit the summary to a maximum of 150 words in total.
    Caution: If there is no information to be extracted, ensure you that your response follows the format 'Based on the recent events, there has been no suitable reasons for the price changes
    News: {news}
    
    """
    try:
        history=[{"role": "model", 'parts': 'Hey....You an Ai agent built mainly price analysis for the pricing domain.Normally , you would be a given a series of summaries of web articles and the date each were published, and you are expected to extract the main factors that affected the  food prices.'},
                 {"role":"user","parts":"prompt"}
                ]
        
        chat_session = model.start_chat(history=history)
        
        response = chat_session.send_message(prompt)
                
        return response.text
    except:
        return ''
    
