from dotenv import load_dotenv

load_dotenv()
import os
import logging
from llama_index.vector_stores import MongoDBAtlasVectorSearch

from app.engine.constants import CHUNK_SIZE
from app.engine.context import create_service_context

from sqlalchemy import create_engine

from llama_index import (
    SimpleDirectoryReader,
    VectorStoreIndex,
    StorageContext,
)

from llama_index.readers import WikipediaReader
from llama_index.node_parser import TokenTextSplitter
from llama_index.vector_stores.types import MetadataInfo, VectorStoreInfo
from llama_index.indices.vector_store import VectorIndexAutoRetriever

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def generate_datasource(service_context):
#     logger.info("Creating new index")
#     # load the documents and create the index
#     documents = SimpleDirectoryReader(DATA_DIR).load_data()
#     store = MongoDBAtlasVectorSearch(
#         db_name=os.environ["MONGODB_DATABASE"],
#         collection_name=os.environ["MONGODB_VECTORS"],
#         index_name=os.environ["MONGODB_VECTOR_INDEX"],
#     )
#     storage_context = StorageContext.from_defaults(vector_store=store)
#     VectorStoreIndex.from_documents(
#         documents,
#         service_context=service_context,
#         storage_context=storage_context,
#         show_progress=True,  # this will show you a progress bar as the embeddings are created
#     )
#     logger.info(
#         f"Successfully created embeddings in the MongoDB collection {os.environ['MONGODB_VECTORS']}"
#     )
#     logger.info(
#         """IMPORTANT: You can't query your index yet because you need to create a vector search index in MongoDB's UI now.
# See https://github.com/run-llama/mongodb-demo/tree/main?tab=readme-ov-file#create-a-vector-search-index"""
#     )
    
    engine = create_engine(os.environ["POSTGRES_URI"])

    node_parser = TokenTextSplitter(chunk_size= CHUNK_SIZE)

    store= MongoDBAtlasVectorSearch(
        db_name = os.environ["MONGODB_DATABASE"],
        collection_name=os.environ["MONGODB_VECTORS"],
    )
    storage_context = StorageContext.from_defaults(vector_store= store)
    vector_index = VectorStoreIndex([], storage_context=storage_context)

    with engine.connect() as cursor:

        uni_name = [x[0] for x in cursor.exec_driver_sql('SELECT DISTINCT uni_name FROM "University"').fetchall()]
        cities = [x[0] for x in cursor.exec_driver_sql('SELECT DISTINCT location FROM "University"').fetchall()]

        uni_docs = WikipediaReader().load_data(pages= uni_name[:-1])
        cities_docs = WikipediaReader().load_data(pages= cities[:-1])
        
        # print(uni_docs, cities_docs, sep="\n\n\n")
    
    # print(node_parser.get_nodes_from_documents(uni_docs))
        
    # for (city, city_node), (uni, uni_node) in zip(zip(cities, cities_nodes), zip(uni_name, uni_nodes)):

    #     city_node.metadata = {"location": city}
    #     uni_node.metadata = {"uni_name": uni}

    # vector_index.insert_nodes(cities_nodes)
    # vector_index.insert_nodes(uni_nodes)
    
    for (city, city_doc), (uni, uni_doc) in zip(zip(cities, cities_docs), zip(uni_name, uni_docs)):
        city_nodes = node_parser.get_nodes_from_documents([city_doc])
        uni_nodes = node_parser.get_nodes_from_documents([uni_doc])

        for node in city_nodes:
            node.metadata = {"location": city}
            # Additional processing specific to city nodes if needed
        
        for node in uni_nodes:
            node.metadata = {"uni_name": uni}
            # Additional processing specific to university nodes if needed

        vector_index.insert_nodes(nodes=city_nodes + uni_nodes)

    vector_store_info = VectorStoreInfo(
        content_info="articles about different universities and their location",
        metadata_info=[
            MetadataInfo(
                name = "location",
                type="str",
                description="location of different universities"
            ),
            MetadataInfo(
                name = "uni_name",
                type="str",
                description="Name of the universitys"
            )
        ]
    )

    VectorIndexAutoRetriever(
        index= vector_index,
        vector_store_info= vector_store_info
    )
    


    
if __name__ == "__main__":
    generate_datasource(create_service_context())
