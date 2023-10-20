# new imports
import pandas as pd
from dagster import get_dagster_logger, Output, MetadataValue
import requests,json
from dagster import asset
# new asset
@asset
def health_emotion():
    url = "http://data-api.eastus.cloudapp.azure.com/api/34d7b37c9b223d3ab3dbdab1635b578e"
    req = requests.get(url)
    item = json.loads(req.text)


    #columns=item['columns']
    #results.append(item)
    #df = pd.DataFrame(results,columns=columns)
    #logger.info(f"got {len(results)} top stories")
    # 提取rows和columns数据
    rows =item["rows"]
    #print(rows[:2],len(rows[0]))
    # rows=list(map(lambda x: [x[0]]+x[1].split(','),data["rows"]))
    #print(rows[:2],len(rows[0]))
    #print(data["columns"])
    columns = item["columns"]
    #print(new_columns)
    # 使用pd.DataFrame构造函数创建DataFrame
    df = pd.DataFrame(rows, columns=columns)
    df['ID'] = df['ID'].astype('int32')
    
    return Output(
        value=df,
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        },
    )
@asset
def health_fetus():
    url = "http://data-api.eastus.cloudapp.azure.com/api/c8bce3d49adaa1c1145a50ccdf962db1"
    req = requests.get(url)
    item = json.loads(req.text)


    #columns=item['columns']
    #results.append(item)
    #df = pd.DataFrame(results,columns=columns)
    #logger.info(f"got {len(results)} top stories")
    # 提取rows和columns数据
    rows =item["rows"]
    #print(rows[:2],len(rows[0]))
    # rows=list(map(lambda x: [x[0]]+x[1].split(','),data["rows"]))
    #print(rows[:2],len(rows[0]))
    #print(data["columns"])
    columns = item["columns"]
    #print(new_columns)
    # 使用pd.DataFrame构造函数创建DataFrame
    df = pd.DataFrame(rows, columns=columns)
    df['ID'] = df['ID'].astype('int32')
    
    return Output(
        value=df,
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        },
    )
@asset
def health_pet():
    url = "http://data-api.eastus.cloudapp.azure.com/api/34d7b37c9b223d3ab3dbdab1635b578e"
    req = requests.get(url)
    item = json.loads(req.text)


    #columns=item['columns']
    #results.append(item)
    #df = pd.DataFrame(results,columns=columns)
    #logger.info(f"got {len(results)} top stories")
    # 提取rows和columns数据
    rows =item["rows"]
    #print(rows[:2],len(rows[0]))
    # rows=list(map(lambda x: [x[0]]+x[1].split(','),data["rows"]))
    #print(rows[:2],len(rows[0]))
    #print(data["columns"])
    columns = item["columns"]
    #print(new_columns)
    # 使用pd.DataFrame构造函数创建DataFrame
    df = pd.DataFrame(rows, columns=columns)
    df['ID'] = df['ID'].astype('int32')
   
    return Output(
        value=df,
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head(10).to_markdown()),
        },
    )