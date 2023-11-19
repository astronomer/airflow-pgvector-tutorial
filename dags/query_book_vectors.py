from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime
from airflow.models.param import Param
from airflow.providers.pgvector.hooks.pgvector import PgVectorHook
from airflow.providers.pgvector.operators.pgvector import PgVectorIngestOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
import uuid
from transformers import AutoTokenizer, AutoModel
from torch import no_grad
import re
import os
from airflow.exceptions import AirflowSkipException


from openai import OpenAI

POSTGRES_CONN_ID = "postgres_default"
TEXT_FILE_PATH = "include/book_data.txt"
TABLE_NAME = "Book"
HUGGING_FACE_MODEL = "consciousAI/cai-lunaris-text-embeddings"


def create_embeddings(text: str, model: str):
    # tokenizer = AutoTokenizer.from_pretrained(model)
    # model = AutoModel.from_pretrained(model)

    # inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True)

    # with no_grad():
    #     outputs = model(**inputs)

    # embeddings = outputs.last_hidden_state.mean(dim=1)

    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    response = client.embeddings.create(input = text, model="text-embedding-ada-002")
    print(response.data[0].embedding)
    embeddings = response.data[0].embedding

    return embeddings #embeddings.tolist()[0]


@dag(
    start_date=datetime(2023, 9, 1),
    schedule=None,
    catchup=False,
    tags=["pgvector"],
    params={
        "book_mood": Param(
            "Exploration of mind and consciousness through neuroscience and philosophical inquiry.",
            type="string",
            description="Describe the kind of book you want to read.",
        ),
    },
)
def query_book_vectors():
    @task
    def import_book_data(text_file_path: str, table_name: str) -> list:
        "Read the text file and create a list of dicts from the book information."
        with open(text_file_path, "r") as f:
            lines = f.readlines()

            num_skipped_lines = 0
            list_of_params = []
            for line in lines:
                parts = line.split(":::")
                title_year = parts[1].strip()
                match = re.match(r"(.+) \((\d{4})\)", title_year)
                try:
                    title, year = match.groups()
                    year = int(year)
                # skip malformed lines
                except:
                    num_skipped_lines += 1
                    continue

                author = parts[2].strip()
                description = parts[3].strip()

                list_of_params.append(
                    {
                        "book_id": str(
                            uuid.uuid5(
                                name=" ".join([title, str(year), author, description]),
                                namespace=uuid.NAMESPACE_DNS,
                            )
                        ),
                        "title": title,
                        "year": year,
                        "author": author,
                        "description": description,
                    }
                )

            print(
                f"Created a list with {len(list_of_params)} elements while skipping {num_skipped_lines} lines."
            )
            return list_of_params

    get_already_imported_book_ids = PostgresOperator(
        task_id="get_already_imported_book_ids",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
        SELECT book_id
        FROM {TABLE_NAME};
        """,
    )

    @task
    def create_embeddings_book_data(
        book_data: dict, model: str, already_imported_books: list
    ) -> dict:
        already_imported_books_ids = [x[0] for x in already_imported_books]
        if book_data["book_id"] in already_imported_books_ids:
            raise AirflowSkipException("Book already imported.")
        embeddings = create_embeddings(text=book_data["description"], model=model)
        book_data["vector"] = embeddings
        return book_data

    @task
    def create_embeddings_query(model: str, **context) -> list:
        query = context["params"]["book_mood"]
        embeddings = create_embeddings(text=query, model=model)
        return embeddings

    @task
    def get_model_vector_length(model: str) -> int:
        model = AutoModel.from_pretrained(model)
        embedding_size = model.config.hidden_size
        return embedding_size

    vector_length = get_model_vector_length(model=HUGGING_FACE_MODEL)
    book_data = import_book_data(text_file_path=TEXT_FILE_PATH, table_name=TABLE_NAME)
    book_embeddings = create_embeddings_book_data.partial(
        model=HUGGING_FACE_MODEL,
        already_imported_books=get_already_imported_book_ids.output,
    ).expand(book_data=book_data)
    query_vector = create_embeddings_query(model=HUGGING_FACE_MODEL)

    enable_vector_extension_if_not_exists = PostgresOperator(
        task_id="enable_vector_extension_if_not_exists",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="CREATE EXTENSION IF NOT EXISTS vector;",
    )

    create_table_if_not_exists = PostgresOperator(
        task_id="create_table_if_not_exists",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (          
            book_id UUID PRIMARY KEY,
            title TEXT,
            year INTEGER,
            author TEXT,
            description TEXT,
            vector VECTOR(%(vector_length)s)
        );
        """,
        parameters={"vector_length": 1536}#vector_length},
    )

    import_embeddings_to_pgvector = PgVectorIngestOperator.partial(
        task_id="import_embeddings_to_pgvector",
        trigger_rule="none_failed",
        conn_id=POSTGRES_CONN_ID,
        sql=(
            f"INSERT INTO {TABLE_NAME} "
            "(book_id, title, year, author, description, vector) "
            "VALUES (%(book_id)s, %(title)s, %(year)s, %(author)s, %(description)s, %(vector)s) "
            "ON CONFLICT (book_id) DO NOTHING;"
        ),
    ).expand(parameters=book_embeddings)

    get_a_book_suggestion = PostgresOperator(
        task_id="get_a_book_suggestion",
        postgres_conn_id=POSTGRES_CONN_ID,
        trigger_rule="none_failed",
        sql=f"""
            SELECT title, year, description
            FROM {TABLE_NAME}
            ORDER BY vector <-> CAST(%(query_vector)s AS VECTOR)
            LIMIT 1;
        """,
        parameters={"query_vector": query_vector},
    )

    @task
    def print_suggestion(query_result, **context):
        query = context["params"]["book_mood"]
        print(f"Book suggestion for '{query}':")
        print(query_result)

    # @task
    # def clean_postgres_cache(conn_id: str):
    #     hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    #     conn = hook.get_conn()
    #     cursor = conn.cursor()
    #     try:
    #         conn.autocommit = True  # Set autocommit to True to avoid transaction block
    #         cursor.execute(f"UPDATE {TABLE_NAME} SET author = 'King' WHERE author = 'Stephen King';")
    #     finally:
    #         cursor.close()
    #         conn.close()

    chain(
        [enable_vector_extension_if_not_exists, vector_length],
        create_table_if_not_exists,
        get_already_imported_book_ids,
        import_embeddings_to_pgvector,
        get_a_book_suggestion,
        print_suggestion(query_result=get_a_book_suggestion.output),
        # clean_postgres_cache(conn_id=POSTGRES_CONN_ID),
    )

    chain(query_vector, get_a_book_suggestion)
    chain(get_already_imported_book_ids, book_embeddings)


query_book_vectors()
