import streamlit as st
import pandas as pd
import numpy as np
import requests
import json
from streamlit_autorefresh import st_autorefresh
import plotly.express as px

# get indexer query parameter from url if it exists
query_params = st.experimental_get_query_params()

import base64
def add_bg_from_local(image_file):
    with open(image_file, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read())
    st.markdown(
    f"""
    <style>
    .stApp {{
        background-image: url(data:image/{"png"};base64,{encoded_string.decode()});
        background-size: cover
    }}
    </style>
    """,
    unsafe_allow_html=True
    )
# could uncomment here to add background image
#add_bg_from_local('space.png')    

# add MIPs logo
st.image("https://thegraph.com/images/mips/mips.png")

# Automatically refresh app every 5 minutes - stops after 25 times
count = st_autorefresh(interval=300000, limit=25, key="fizzbuzzcounter")

@st.cache(allow_output_mutation=True)
def get_subgraph_info(total_rows):
    # Initialize an empty list to store the results
    results = []
    # Set the number of requests to make
    num_requests = total_rows // 1000
    # Make the GraphQL requests
    for i in range(num_requests):
        # Set the GraphQL query
        query = '''
        query {
          subgraphs(
            where: {active: true}
            first: 1000
            orderBy: signalledTokens
            orderDirection: desc
            skip: ''' + str(i*1000) + '''
          ) {
            displayName
            signalledTokens
            creatorAddress
            versions(first: 1, orderBy: createdAt, orderDirection: desc) {
              subgraphDeployment {
                ipfsHash
              }
            }
          }
        }
        '''
        # Send the GraphQL request
        r = requests.post("https://api.thegraph.com/subgraphs/name/graphprotocol/graph-network-mainnet", json={'query': query})
        # Load result into json
        json_data = json.loads(r.text)
        #st.write(json_data)
        # Convert json into a dataframe
        df = pd.DataFrame(json_data['data']['subgraphs'])
        # Add the dataframe to the list
        results.append(df)
    # Union the dataframes into a single dataframe
    df = pd.concat(results)
    # Return the results
    return df
# pull subgraphs info
subgraphs_info = get_subgraph_info(1000).drop_duplicates(subset=['displayName', 'signalledTokens', 'creatorAddress'])

# Iterate through the data and extract the ipfsHash values
ipfs_hash_values = []
for i in subgraphs_info.index:
  ipfs_hash_values.append(subgraphs_info['versions'][i][0]['subgraphDeployment']['ipfsHash'])
# Set the ipfsHash values as a new column in the data structure
subgraphs_info['ipfsHash'] = ipfs_hash_values
# Drop the versions column
del subgraphs_info['versions']

# Markdown title
st.write('## Quality of Service Daily Data (All Indexers)')

# create column which takes subgraph name, but uses ipfs hash when it doesn't exist
subgraphs_info['subgraph'] = subgraphs_info['displayName'].where(subgraphs_info['displayName'].notnull(), subgraphs_info['ipfsHash'])

# find index of datanexus as default example for selection
#default_subgraph = int(subgraphs_info["subgraph"].str.find("POAP Ethereum Mainnet")[lambda x : x != -1].index[0])
# choose indexer
#with st.sidebar:
  # number of rows to pull user input
  #nrows = st.slider('How many rows of data do you want to pull? One observation per subgraph every 5 minutes', 1000, 50000, 3000, 1000)

# initialize text of how many rows have been pulled
t = st.empty()
@st.cache(suppress_st_warning=True)
def pull_data(nrows):
  # Initialize an empty list to store the dataframes
  df_list = []
  # Initialize the minimum end_epoch value to a very large number
  min_epoch = 999999999999999999
  
  # Get data for the indexer (30k rows)
  for i in range(int(nrows/1000)):
      if i == 0:
        skip = 0
      else:
        skip = i*1000
      # Display the updated text using the st.cache function
      #t.markdown(str("#### Now pulled " + str(i*1+1) + ",000 rows from subgraph"))
      # Get data for the indexer
      query = str('''{
        indexerDailyDataPoints(orderBy: end_epoch, orderDirection: desc, where:{subgraph_deployment_ipfs_hash: "QmXWbpH76U6TM4teRNMZzog2ismx577CkH7dzn1Nw69FcV"}, first: 1000, skip: '''+str(skip)+'''){
          dayStart
          dayEnd
          indexer_url
          indexer_wallet
          subgraph_deployment_ipfs_hash
          avg_indexer_blocks_behind
          avg_indexer_latency_ms
          avg_query_fee
          max_indexer_blocks_behind
          max_indexer_latency_ms
          max_query_fee
          num_indexer_200_responses
          proportion_indexer_200_responses
          query_count
          start_epoch
          total_query_fees
          }
      }''')
      # Set endpoint url
      url = 'https://api.thegraph.com/subgraphs/id/QmVCub6KrGUCGaBGZJatw96KHeisJdy8pDbZJdV4z3icg4'
      r = requests.post(url, json={'query': query})
      # Load result into json
      json_data = json.loads(r.text)
      #st.write(json_data)
      # Convert json into a dataframe
      df = pd.DataFrame(json_data['data']['indexerDailyDataPoints'])
      # Convert unix timestamp to date
      df['day_start'] = pd.to_datetime(df['dayStart'],unit='s')
      # Update the minimum end_epoch value
      min_epoch = min(min_epoch, df['dayStart'].astype(int).min())
      # Add the dataframe to the list
      df_list.append(df)
  return df_list
# pull data
df_list = pull_data(6000)
# Union the dataframes into a single dataframe
df = pd.concat(df_list)

# Join subgraphs_info into new data
df = pd.merge(left=df, right=subgraphs_info, left_on='subgraph_deployment_ipfs_hash', right_on='ipfsHash', how='inner')
# create column which takes subgraph name, but uses ipfs hash when it doesn't exist
df['subgraph'] = df['displayName'].where(df['displayName'].notnull(), df['subgraph_deployment_ipfs_hash'])

# only keep select columns
df = df[['subgraph', 'day_start', 'indexer_wallet', 'indexer_url', 'query_count', 'num_indexer_200_responses', 'proportion_indexer_200_responses', 'avg_indexer_latency_ms', 'avg_indexer_blocks_behind', 'avg_query_fee', 'max_indexer_latency_ms', 'max_indexer_blocks_behind', 'max_query_fee', 'total_query_fees']]

# show data (only if data is less than 15k rows)
if df.shape[0] < 15000:
  #st.write("Daily Interval Data (All Indexers)")
  st.dataframe(df.style.hide_index())

# Download data button
@st.cache
def convert_df(df):
    # Caches the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')
# convert
csv = convert_df(df)
# show button
st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name='subgraph_indexer_data.csv',
    mime='text/csv',
)

st.write('### Visualization - All Indexers')

col_viz = st.selectbox('Which column do you want to visualize?', 
                        ('query_count','avg_indexer_blocks_behind','avg_indexer_latency_ms',
                        'avg_query_fee','max_indexer_blocks_behind','max_indexer_latency_ms',
                        'max_query_fee','num_indexer_200_responses','proportion_indexer_200_responses',
                        #'query_count',#'stdev_indexer_latency_ms',
                        'total_query_fees'))
# time interval
#time_interval = st.selectbox('Choose a time interval', ('1 hour', '5 minutes'))
# chart type
chart_type = st.selectbox('Choose chart type', ('bar', 'line', 'area', 'scatter', 'pie'))

st.write("Daily data of `" + col_viz + "` for subgraph Connext Network - Gnosis" + " from " + str(df['day_start'].min()) + " to " + str(df['day_start'].max()))
                          
# Convert column to numeric
df[col_viz] = pd.to_numeric(df[col_viz])

# visualizations:
if chart_type != 'pie':
  fig = getattr(px, chart_type)(
    df,
    x="day_start",
    y=col_viz,
    # size="pop",
    color="indexer_url",
    hover_name="indexer_url")
  # fig.update_layout(showlegend=False)
  st.plotly_chart(fig, theme="streamlit", use_container_width=True)
  # table
  #st.dataframe(data_viz.groupby([data_viz['hour'], 'indexer_url']).max_query_fee.max().reset_index(name=col_viz))

if chart_type == 'pie':
  if col_viz == 'query_count' or col_viz == 'num_indexer_200_responses' or col_viz == 'total_query_fees':
    fig = px.pie(df, values=col_viz, names='indexer_url', title=col_viz + " by indexer url from " + str(df['day_start'].min()) + " to " + str(df['day_start'].max()))
    # add labels inside (commented out for now)
    #fig.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
  else:
    st.write('column not compatible with pie chart - please select a different column to visualize')


st.write('## Specific Indexer')

# set default indexer from url
indexer_default = query_params["indexer"][0] if "indexer" in query_params else 0
indexers_list = df['indexer_wallet'].drop_duplicates().tolist()
# if url selection exists then make it the default (by being first option of the list)
if indexer_default != 0:
  # make first option the one from url
  indexers_list.insert(0,indexer_default)
  indexer_filter = st.selectbox('Which indexer do you want to visualize?', indexers_list)
else:
  indexer_filter = st.selectbox('Which indexer do you want to visualize?', indexers_list)



# Get data for the indexer
query = str('''{
  indexerDailyDataPoints(orderBy: end_epoch, orderDirection: desc, where:{subgraph_deployment_ipfs_hash: "QmXWbpH76U6TM4teRNMZzog2ismx577CkH7dzn1Nw69FcV", indexer_wallet: "'''+indexer_filter+'''"}, first: 1000){
    dayStart
    dayEnd
    indexer_url
    indexer_wallet
    subgraph_deployment_ipfs_hash
    avg_indexer_blocks_behind
    avg_indexer_latency_ms
    avg_query_fee
    max_indexer_blocks_behind
    max_indexer_latency_ms
    max_query_fee
    num_indexer_200_responses
    proportion_indexer_200_responses
    query_count
    start_epoch
    total_query_fees
    }
}''')
# Set endpoint url
url = 'https://api.thegraph.com/subgraphs/id/QmVCub6KrGUCGaBGZJatw96KHeisJdy8pDbZJdV4z3icg4'
r = requests.post(url, json={'query': query})
# Load result into json
json_data = json.loads(r.text)
#st.write(json_data)
# Convert json into a dataframe
indexer_df = pd.DataFrame(json_data['data']['indexerDailyDataPoints'])
# Convert unix timestamp to date
indexer_df['day_start'] = pd.to_datetime(indexer_df['dayStart'],unit='s')


# Join subgraphs_info into new data
indexer_df = pd.merge(left=indexer_df, right=subgraphs_info, left_on='subgraph_deployment_ipfs_hash', right_on='ipfsHash', how='inner')
# create column which takes subgraph name, but uses ipfs hash when it doesn't exist
indexer_df['subgraph'] = indexer_df['displayName'].where(indexer_df['displayName'].notnull(), indexer_df['subgraph_deployment_ipfs_hash'])

# only keep select columns
indexer_df = indexer_df[['subgraph', 'day_start', 'indexer_wallet', 'indexer_url', 'query_count', 'num_indexer_200_responses', 'proportion_indexer_200_responses', 'avg_indexer_latency_ms', 'avg_indexer_blocks_behind', 'avg_query_fee', 'max_indexer_latency_ms', 'max_indexer_blocks_behind', 'max_query_fee', 'total_query_fees']]

# show data:
st.write("Daily Interval Data for Indexer: " + indexer_filter)
st.dataframe(indexer_df.style.hide_index())

# Download data button
# convert
csvtwo = convert_df(indexer_df)
# show button
st.download_button(
    label="Download indexer data as CSV",
    data=csvtwo,
    file_name='subgraph_indexer_data_'+indexer_filter+'.csv',
    mime='text/csv',
)


st.write('### Visualization - Specific Indexer')


col_viz_two = st.selectbox('Which column do you want to visualize?', 
                        ('query_count','avg_indexer_blocks_behind','avg_indexer_latency_ms',
                        'avg_query_fee','max_indexer_blocks_behind','max_indexer_latency_ms',
                        'max_query_fee','num_indexer_200_responses','proportion_indexer_200_responses',
                        #'query_count',#'stdev_indexer_latency_ms',
                        'total_query_fees'), key = 1111)
# time interval
#time_interval = st.selectbox('Choose a time interval', ('1 hour', '5 minutes'))
# chart type
chart_type_two = st.selectbox('Choose chart type', ('line', 'bar', 'area', 'scatter', 'pie'))

st.write("Daily data of `" + col_viz_two + "` for subgraph Connext Network - Gnosis" + " from " + str(indexer_df['day_start'].min()) + " to " + str(indexer_df['day_start'].max()))
                          
# Convert column to numeric
indexer_df[col_viz_two] = pd.to_numeric(indexer_df[col_viz_two])

# visualizations:
if chart_type_two != 'pie':
  fig = getattr(px, chart_type_two)(
    indexer_df,
    x="day_start",
    y=col_viz_two,
    # size="pop",
    color="indexer_url",
    hover_name="indexer_url")
  # fig.update_layout(showlegend=False)
  st.plotly_chart(fig, theme="streamlit", use_container_width=True)
  # table
  #st.dataframe(data_viz.groupby([data_viz['hour'], 'indexer_url']).max_query_fee.max().reset_index(name=col_viz))

if chart_type_two == 'pie':
  if col_viz_two == 'query_count' or col_viz_two == 'num_indexer_200_responses' or col_viz_two == 'total_query_fees':
    fig = px.pie(indexer_df, values=col_viz_two, names='indexer_url', title=col_viz_two + " by indexer url from " + str(indexer_df['day_start'].min()) + " to " + str(indexer_df['day_start'].max()))
    # add labels inside (commented out for now)
    #fig.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
  else:
    st.write('column not compatible with pie chart - please select a different column to visualize')










