from pyspark.sql import types as spark_types

class SharePointSchemaProvider:
    __ALLOWED_TYPES = [
        "text",
        "person",
        "lookup"
        "boolean",
        "dateTime",
        "date",
        "integer",
        "decimal",
        "url"
    ]

    __PRIMITIVE_TYPES = [
        "text",
        "boolean",
        "dateTime",
        "date",
        "integer",
        "decimal"
    ]

    __ALLOWED_FIELDS = [
        "text",
        "boolean",
        "dateTime",
        "date",
        "integer",
        "decimal"
    ]

    __SHAREPOINT_TO_SPARK_TYPES_MAP = {
        "text": spark_types.StringType(),
        "boolean": spark_types.BooleanType(),
        "dateTime": spark_types.TimestampType(),
        "date": spark_types.DateType(),
        "integer": spark_types.IntegerType(),
        "decimal": spark_types.DecimalType()
    }
    
    def __init__(self, schema):
        self.__schema = schema
        self.__select_query = ""
        self.__expand_query = ""
        self.__spark_schema = None
        self.__generated = False

    def __generate(self):
        select_array = []
        expand_array = []         
        spark_schema_columns = []
    
        for col in self.__schema:
        
            if not col["type"] in self.__ALLOWED_TYPES:
                raise "Error"
            
            # primitive types
            if col["type"] in self.__PRIMITIVE_TYPES:
                select_array.append(col["srcName"])
                spark_schema_columns.append(
                    spark_types.StructField(
                        col["srcName"],
                        self.__SHAREPOINT_TO_SPARK_TYPES_MAP[col["type"]],
                        col["nullable"]
                    )
                )

            # complex types
            else:
                expand_array.append(col["srcName"])
                spark_schema_fields = []
                for field in col["fields"]:
                    if not field["type"] in self.__ALLOWED_FIELDS:
                        raise "Error"
                    select_array.append(f"{col['srcName']}/{field['srcName']}")
                    spark_schema_fields.append(
                        spark_types.StructField(
                            field["srcName"],
                            self.__SHAREPOINT_TO_SPARK_TYPES_MAP[field["type"]],
                            field["nullable"]
                        )
                    )
                spark_schema_columns.append(
                    spark_types.StructField(
                        col["srcName"],
                        spark_types.StructType(spark_schema_fields),
                        col["nullable"]
                    )
                )

        self.__spark_schema = spark_types.StructType(spark_schema_columns)
        self.__select_query = ",".join(select_array)
        self.__expand_query = ",".join(expand_array)
        self.__generated = True

    def get_spark_schema(self):
        if not self.__generated: self.__generate()
        return self.__spark_schema

    def get_api_select_query(self):
        if not self.__generated: self.__generate()
        return self.__select_query

    def get_api_expand_query(self):
        if not self.__generated: self.__generate()
        return self.__expand_query