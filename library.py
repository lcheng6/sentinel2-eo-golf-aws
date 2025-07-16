import pystac_client
import planetary_computer
import json
import requests
import rasterio
from io import BytesIO
from matplotlib import pyplot
from rasterio.io import MemoryFile
import rasterio
from matplotlib import pyplot
from rasterio.plot import show

from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from pyspark.sql.types import ArrayType, StringType, MapType, DoubleType, IntegerType, StringType, StructType, StructField

from pyspark.sql.functions import pandas_udf
import pandas as pd

catalog =  pystac_client.Client.open(
  "https://planetarycomputer.microsoft.com/api/stac/v1",
  modifier=planetary_computer.sign_inplace
)


@udf(ArrayType(StringType()))
def get_assets(item):
  item_dict = json.loads(item)
  assets = item_dict["assets"]
  return [json.dumps({**{"name": asset}, **assets[asset]}) for asset in assets]


@udf(ArrayType(StringType()))
def get_items(geojson, datetime, collections):

  from tenacity import retry, wait_exponential

  @retry(wait=wait_exponential(multiplier=2, min=4, max=120))
  def search_with_retry(geojson, catalog, collection, dt):
    search = catalog.search(
        collections = collection,
        intersects = geojson,
        datetime = dt,
        query={"eo:cloud_cover": {"lt": 25}}
      )
    items = search.item_collection()
    return [json.dumps(item.to_dict()) for item in items]

  def search_catalog(geojson, catalog, collection, dt):
    try:
      return search_with_retry(geojson, catalog, collection, dt)
    except Exception as inst:
      return [str(inst)]

  catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace
  )

  dt = datetime
  coll = collections
  return search_catalog(geojson, catalog, coll, dt)


def get_assets_for_cells(cells_df, period, source):
  return cells_df\
    .withColumn("items", get_items("geojson", F.lit(period), F.array(F.lit(source))))\
    .repartition(200, F.rand())\
    .withColumn("items", F.explode("items"))\
    .withColumn("assets", get_assets("items"))\
    .repartition(200, F.rand())\
    .withColumn("assets", F.explode("assets"))\
    .withColumn("asset", F.from_json(F.col("assets"), MapType(StringType(), StringType())))\
    .withColumn("item", F.from_json(F.col("items"), MapType(StringType(), StringType())))\
    .withColumn("item_properties", F.from_json("item.properties", MapType(StringType(), StringType())))\
    .withColumn("item_collection", F.col("item.collection"))\
    .withColumn("item_bbox", F.col("item.bbox"))\
    .withColumn("item_id", F.col("item.id"))\
    .withColumn("stac_version", F.col("item.stac_version"))\
    .drop("assets", "items", "item")\
    .repartition(200, F.rand())


@udf(StringType())
def download_asset(href, dir_path):
  import requests
  import os.path
  from tenacity import retry, wait_exponential, TryAgain, stop_after_attempt, RetryError

  filename = os.path.basename(href.split("?")[0])
  outpath = f"{dir_path}/{filename}"
  if os.path.exists(outpath):
    return outpath
  
  @retry(
    wait=wait_exponential(multiplier=2, min=4, max=120),
    stop=stop_after_attempt(5)
    )
  def retryable(href, dir_path, filename):
    try:
      print(f"Downloading {filename} from {href} to {outpath}")
      # Make the actual request, set the timeout for no data to 10 seconds and enable streaming responses so we don't have to keep the large files in memory
      href = href.split("?")[0]
      signed_url = planetary_computer.sign_url(href)
      response = requests.get(signed_url, timeout=100, stream=True)
      if int(response.status_code) != 200 or int(response.headers['content-length']) < 1024:
        print(f"Downloading {filename} from {href} failed. Trying again.")
        raise TryAgain
      # Open the output file and make sure we write in binary mode
      with open(outpath, 'wb') as fh:
        # Walk through the request response in chunks of 1024 * 1024 bytes, so 1MiB
        for chunk in response.iter_content(1024 * 1024):
          # Write the chunk to the file
          fh.write(chunk)
          # Optionally we can check here if the download is taking too long
      return outpath
    except RetryError as e:
      return f"Error downloading {filename}: {e}"
    except:
      raise TryAgain
  return retryable(href, dir_path, filename)
  

def plot_raster(raster):
  fig, ax = pyplot.subplots(1, figsize=(16, 16))

  with MemoryFile(BytesIO(raster)) as memfile:
    with memfile.open() as src:
      show(src, ax=ax)
      pyplot.show()


def plot_file(file_path):
  fig, ax = pyplot.subplots(1, figsize=(16, 16))

  with rasterio.open(file_path) as src:
    show(src, ax=ax)
    pyplot.show()


def rasterio_lambda(raster, lambda_f):
  @udf("double")
  def f_udf(f_raster):
    with MemoryFile(BytesIO(f_raster)) as memfile:
      with memfile.open() as dataset:
        x = lambda_f(dataset)
        return float(x)
  
  return f_udf(raster)