# define imported data schema
shp_items_schema = [
    {"srcName": "Id", "type": "integer", "nullable": False},
    {"srcName": "Title", "type": "text", "nullable": True},
    {"srcName": "Author", "type": "person", "nullable": False, "fields": [
        {"srcName": "EMail", "type": "text", "nullable": False},
    ]},
    {"srcName": "Company", "type": "lookup", "nullable": True, "fields": [
        {"srcName": "Title", "type": "text", "nullable": False}
    ]}
]

shp_items_schema_provider = SharePointSchemaProvider(shp_items_schema)

shp_client = SharePointApiClient(
    "org-domain",
    "tenant-id",
    client_id,
    client_secret
)

# fetch data
items = shp_client.get_all_list_items(
    "site-name",
    "list-id",
    select = shp_items_schema_provider.get_api_select_query(),
    expand = shp_items_schema_provider.get_api_expand_query() 
)

# compose pyspark dataframe
items_df = spark.createDataFrame(items, schema = shp_items_schema_provider.get_spark_schema())