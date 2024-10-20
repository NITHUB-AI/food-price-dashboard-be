# Add OpenAI library
import openai
from openai import AzureOpenAI

import os

from dotenv import load_dotenv
load_dotenv()

openai.api_key = os.getenv('API_KEY')
openai.api_base =  os.getenv('ENDPOINT')
openai.api_type = 'azure' # Necessary for using the OpenAI library with Azure OpenAI
openai.api_version = '2023-05-15' # Latest / target version of the API

deployment_name = 'Voicetask' # SDK calls this "engine", but naming
                                           # it "deployment_name" for clarity
                                           
client = AzureOpenAI(
    api_version=openai.api_version,
    azure_endpoint=openai.api_base,
    azure_deployment=deployment_name,
)

def summarize(news, model="gpt-3.5-turbo", deployment_name='Voicetask'):

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