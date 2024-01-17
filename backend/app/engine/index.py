import logging
import os

from sqlalchemy import create_engine

from llama_index import (
    VectorStoreIndex,
    SQLDatabase
)
from llama_index.vector_stores import MongoDBAtlasVectorSearch
from llama_index.indices.struct_store.sql_query import NLSQLTableQueryEngine
from llama_index.tools import QueryEngineTool, ToolMetadata
from llama_index.agent import ReActAgent, OpenAIAgentWorker, AgentRunner, OpenAIAgent

from app.engine.context import create_service_context



def get_chat_engine():
    service_context = create_service_context()
    logger = logging.getLogger("uvicorn")
    logger.info("Connecting to index from MongoDB...")
    store = MongoDBAtlasVectorSearch(
        db_name=os.environ["MONGODB_DATABASE"],
        collection_name=os.environ["MONGODB_VECTORS"],
        index_name=os.environ["MONGODB_VECTOR_INDEX"],
    )
    
    vector_index = VectorStoreIndex.from_vector_store(store, service_context)
    vector_query_engine = vector_index.as_query_engine(similarity_top_k=20)
    logger.info("Finished connecting to index from MongoDB.")

    engine = create_engine(os.environ['POSTGRES_URI'])
    sql_db = SQLDatabase(
        engine= engine,
        include_tables= ["University", "Programme", "ProgrammeDescription", "TestType"]
    )

    # sql_db._engine.
    sql_query_engine = NLSQLTableQueryEngine(
        sql_database= sql_db,
        tables= ["University", "Programme", "ProgrammeDescription", "TestType"]
    )

    query_engine_tools = [
        QueryEngineTool(
            query_engine= sql_query_engine,
            metadata= ToolMetadata(
                name="University_DB",
                description='''
                    Useful for translating a natural language query into a SQL query over"
                    different tables containing information about different programs at "
                    each universityy
                    '''
            )
        ),
        QueryEngineTool(
            query_engine= vector_query_engine,
            metadata=  ToolMetadata(
                name = "Location and University",
                description= "helps in giving historical, geographical, cultural information about the university and where its located."
            )
        )
    ]

    # agent = ReActAgent.from_tools(query_engine_tools, llm = service_context.llm, verbose= True)
    # agent = AgentRunner(agent)
    
    
    return OpenAIAgent.from_tools(query_engine_tools, llm = service_context.llm, verbose= True)
