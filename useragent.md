

```python
import os
os.listdir(os.getcwd())
```




    ['.ipynb_checkpoints',
     'derby.log',
     'useragent.ipynb',
     'spark-warehouse',
     'metastore_db']




```python
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
import pyspark.sql.types as t
import re

```


```python
sparkSession = (SparkSession
                .builder
                .appName('example')
                .master('local')
                .enableHiveSupport()
                .getOrCreate())
```


```python
#Check if hive databases are available
sparkSession.sql("""
     show databases
""").toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>databaseName</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>default</td>
    </tr>
    <tr>
      <th>1</th>
      <td>logs</td>
    </tr>
  </tbody>
</table>
</div>




```python
sparkSession.catalog.listDatabases()
```




    [Database(name='default', description='Default Hive database', locationUri='file:/home/jovyan/useragent/spark-warehouse'),
     Database(name='logs', description='', locationUri='file:/home/jovyan/useragent/spark-warehouse/logs.db')]




```python
#Check tables in a database
sparkSession.sql("""
     show tables in default
""").toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>database</th>
      <th>tableName</th>
      <th>isTemporary</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>




```python
sparkSession.catalog.listTables("default")

```




    []




```python
sparkSession.sql('create database IF NOT EXISTS logs')
```




    DataFrame[]




```python
sparkSession.catalog.listDatabases()
```




    [Database(name='default', description='Default Hive database', locationUri='file:/home/jovyan/useragent/spark-warehouse'),
     Database(name='logs', description='', locationUri='file:/home/jovyan/useragent/spark-warehouse/logs.db')]




```python
# Define schema
schema = StructType([
    StructField("ip", StringType(), True),
    StructField("clientID", StringType(), True),
    StructField("userID", StringType(), True),
    StructField("time", StringType(), True),
    StructField("method", StringType(), True),
    StructField("endpoint", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("response", IntegerType(), True),
    StructField("size", IntegerType(), True),
    StructField("user_agent", StringType(), True)
])
```


```python
useragent_from_csv_df = sparkSession\
 .read.csv("../data/user_agent.csv", schema)
type(useragent_from_csv_df)    
```




    pyspark.sql.dataframe.DataFrame




```python
sparkSession.sql('use logs')
useragent_from_csv_df.write.mode('overwrite').saveAsTable("useragent")
```


```python
sparkSession.catalog.listTables("logs")
```




    [Table(name='useragent', database='logs', description=None, tableType='MANAGED', isTemporary=False)]




```python
#query against the hive table
sparkSession.sql("""
select *
from useragent

""").limit(30).toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ip</th>
      <th>clientID</th>
      <th>userID</th>
      <th>time</th>
      <th>method</th>
      <th>endpoint</th>
      <th>protocol</th>
      <th>response</th>
      <th>size</th>
      <th>user_agent</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:05:49-0800</td>
      <td>GET</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
      <td>HTTP/1.1</td>
      <td>401</td>
      <td>12846</td>
      <td>Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKi...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:06:51 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1....</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>4523</td>
      <td>Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:10:02 -0800</td>
      <td>GET</td>
      <td>/mailman/listinfo/hsdivision</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>6291</td>
      <td>Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:11:58 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/view/TWiki/WikiSyntax</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>7352</td>
      <td>Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>64.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:15:05:49 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
      <td>HTTP/1.1</td>
      <td>401</td>
      <td>12846</td>
      <td>None</td>
    </tr>
    <tr>
      <th>5</th>
      <td>64.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:15:06:51 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1....</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>4523</td>
      <td>Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>64.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:15:10:02 -0800</td>
      <td>GET</td>
      <td>/mailman/listinfo/hsdivision</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>6291</td>
      <td>Mozilla/5.0 (Windows22; U; Windows NT 5.1; en-...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>64.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:15:11:58 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/view/TWiki/WikiSyntax</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>7352</td>
      <td>Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKi...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>68.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:14:05:49 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
      <td>HTTP/1.1</td>
      <td>401</td>
      <td>12846</td>
      <td>None</td>
    </tr>
    <tr>
      <th>9</th>
      <td>68.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:14:06:51 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1....</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>4523</td>
      <td>Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US...</td>
    </tr>
    <tr>
      <th>10</th>
      <td>68.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:14:10:02 -0800</td>
      <td>GET</td>
      <td>/mailman/listinfo/hsdivision</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>6291</td>
      <td>Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US...</td>
    </tr>
    <tr>
      <th>11</th>
      <td>68.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:14:11:58 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/view/TWiki/WikiSyntax</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>7352</td>
      <td>Mozilla/5.0   (compatible;    bingbot/2.0;    ...</td>
    </tr>
  </tbody>
</table>
</div>




```python
useragent_from_csv_df.limit(10).toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ip</th>
      <th>clientID</th>
      <th>userID</th>
      <th>time</th>
      <th>method</th>
      <th>endpoint</th>
      <th>protocol</th>
      <th>response</th>
      <th>size</th>
      <th>user_agent</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:05:49-0800</td>
      <td>GET</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
      <td>HTTP/1.1</td>
      <td>401</td>
      <td>12846</td>
      <td>Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKi...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:06:51 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1....</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>4523</td>
      <td>Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:10:02 -0800</td>
      <td>GET</td>
      <td>/mailman/listinfo/hsdivision</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>6291</td>
      <td>Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:11:58 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/view/TWiki/WikiSyntax</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>7352</td>
      <td>Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>64.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:15:05:49 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
      <td>HTTP/1.1</td>
      <td>401</td>
      <td>12846</td>
      <td>None</td>
    </tr>
    <tr>
      <th>5</th>
      <td>64.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:15:06:51 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1....</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>4523</td>
      <td>Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>64.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:15:10:02 -0800</td>
      <td>GET</td>
      <td>/mailman/listinfo/hsdivision</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>6291</td>
      <td>Mozilla/5.0 (Windows22; U; Windows NT 5.1; en-...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>64.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:15:11:58 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/view/TWiki/WikiSyntax</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>7352</td>
      <td>Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKi...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>68.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:14:05:49 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
      <td>HTTP/1.1</td>
      <td>401</td>
      <td>12846</td>
      <td>None</td>
    </tr>
    <tr>
      <th>9</th>
      <td>68.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:14:06:51 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1....</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>4523</td>
      <td>Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US...</td>
    </tr>
  </tbody>
</table>
</div>




```python
#define parse_user_agent

def parse_user_agent(user_agent):
    
    if user_agent is not None: 
        user_agent = re.sub("/?[\d_.]+", "", user_agent)
        user_agent = re.sub("[;\(\):,]", "", user_agent)
        user_agent = user_agent.split()
    
    return user_agent

parse_user_agent_udf = f.udf(parse_user_agent, t.ArrayType(t.StringType()))
```


```python
useragent_from_csv_df.select(parse_user_agent_udf("user_agent").alias("words"))\
                     .select(f.explode("words").alias("word"))\
                     .groupBy("word")\
                     .agg(f.count("*").alias("count"))\
                     .orderBy(f.col("count"))\
                     .toPandas()
    
                            
    
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>word</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>compatible</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>+http//wwwbingcom/bingbothtm</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ABCD</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>bingbot</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>WOW</td>
      <td>2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>U</td>
      <td>7</td>
    </tr>
    <tr>
      <th>6</th>
      <td>en-US</td>
      <td>7</td>
    </tr>
    <tr>
      <th>7</th>
      <td>KHTML</td>
      <td>8</td>
    </tr>
    <tr>
      <th>8</th>
      <td>NT</td>
      <td>9</td>
    </tr>
    <tr>
      <th>9</th>
      <td>AppleWebKit</td>
      <td>9</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Mozilla</td>
      <td>10</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Windows</td>
      <td>16</td>
    </tr>
  </tbody>
</table>
</div>


