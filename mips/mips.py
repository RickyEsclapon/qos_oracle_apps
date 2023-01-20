import streamlit as st
import pandas as pd
import numpy as np
import requests
import json
from streamlit_autorefresh import st_autorefresh
import plotly.express as px

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
st.title('MIPs Quality of Service')

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
def pull_data():
  # Initialize an empty list to store the dataframes
  df_list = []
  # Initialize the minimum end_epoch value to a very large number
  min_epoch = 999999999999999999
  
  # Get data for the indexer (30k rows)
  for i in range(1):
      if i == 0:
        skip = 0
      else:
        skip = i*1000
      # Display the updated text using the st.cache function
      t.markdown(str("#### Now pulled " + str(i*1+1) + ",000 rows from subgraph"))
      # Get data for the indexer
      query = str('''{
        indexerDailyDataPoints(orderBy: end_epoch, orderDirection: desc, where:{subgraph_deployment_ipfs_hash: "QmXWbpH76U6TM4teRNMZzog2ismx577CkH7dzn1Nw69FcV"}, first: 1000, skip: '''+str(skip)+'''){
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
      df['date'] = pd.to_datetime(df['dayEnd'],unit='s')
      # Update the minimum end_epoch value
      min_epoch = min(min_epoch, df['dayEnd'].astype(int).min())
      # Add the dataframe to the list
      df_list.append(df)
  return df_list
# pull data
df_list = pull_data()
# Union the dataframes into a single dataframe
df = pd.concat(df_list)

# Join subgraphs_info into new data
df = pd.merge(left=df, right=subgraphs_info, left_on='subgraph_deployment_ipfs_hash', right_on='ipfsHash', how='inner')
# create column which takes subgraph name, but uses ipfs hash when it doesn't exist
df['subgraph'] = df['displayName'].where(df['displayName'].notnull(), df['subgraph_deployment_ipfs_hash'])

# show data (only if data is less than 15k rows)
if df.shape[0] < 15000:
  st.write("Daily Interval Data")
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

# Column to visualize
with st.sidebar:
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
                          
# Convert column to numeric
df[col_viz] = pd.to_numeric(df[col_viz])

# 1 hour interval data
if chart_type != 'pie':
  st.write("Daily data of `" + col_viz + "` for subgraph Connext Network - Gnosis" + " from " + str(df['date'].min()) + " to " + str(df['date'].max()))
  # visualize
  fig = getattr(px, chart_type)(
    df,
    x="date",
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
    fig = px.pie(df, values=col_viz, names='indexer_url', title=col_viz + " by indexer url from " + str(df['date'].min()) + " to " + str(df['date'].max()))
    # add labels inside (commented out for now)
    #fig.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
  else:
    st.write('column not compatible with pie chart - please select a different column to visualize')

