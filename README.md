Orchestrate pgvector operations with Apache Airflow
===================================================

This repository contains the DAG code used in the [Orchestrate pgvector operations with Apache Airflow tutorial](https://docs.astronomer.io/learn/airflow-pgvector). 

The DAG in this repository uses the following package:

- [Airflow pgvector provider](https://airflow.apache.org/docs/apache-airflow-providers-pgvector/stable/index.html). 
- [OpenAI python client](https://platform.openai.com/docs/api-reference)

# How to use this repository

This section explains how to run this repository with Airflow. Note that you will need to copy the contents of the `.env_example` file to a newly created `.env` file.
The Postgres connection defined in the `.env_example` file will connect to the secondary Postgres database that is created when you run `astro dev start`. This secondary Postgres database already has `pgvector` installed.

You will need to provide an [OpenAI API key of at least tier 1](https://platform.openai.com/docs/guides/rate-limits/) in the `.env` file to use OpenAI for embeddings. If you do not have an OpenAI API key you can change the code in the `create_embeddings` function in the `query_book_vecotrs.py` file to use a different embedding method (note that you will likely also need to adjust the `MODEL_VECTOR_LENGTH` if you do this).

Download the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) to run Airflow locally in Docker. `astro` is the only package you will need to install locally.

1. Run `git clone https://github.com/astronomer/airflow-pgvector-tutorial.git` on your computer to create a local clone of this repository.
2. Install the Astro CLI by following the steps in the [Astro CLI documentation](https://docs.astronomer.io/astro/cli/install-cli). Docker Desktop/Docker Engine is a prerequisite, but you don't need in-depth Docker knowledge to run Airflow with the Astro CLI.
3. Run `astro dev start` in your cloned repository.
4. After your Astro project has started. View the Airflow UI at `localhost:8080`.

In this project `astro dev start` spins up 5 Docker containers:

- The Airflow webserver, which runs the Airflow UI and can be accessed at `https://localhost:8080/`.
- The Airflow scheduler, which is responsible for monitoring and triggering tasks.
- The Airflow triggerer, which is an Airflow component used to run deferrable operators.
- The Airflow metadata database, which is a Postgres database that runs on port 5432.
- A second local postgres database with pgvector installed, which runs on port 5433.

## Resources

- [Orchestrate pgvector operations with Apache Airflow](https://docs.astronomer.io/learn/airflow-pgvector).
- [Airflow Weaviate provider documentation](https://airflow.apache.org/docs/apache-airflow-providers-pgvector/stable/index.html).
- [pgvector repository](https://github.com/pgvector/pgvector).
- [OpenAI API reference](https://platform.openai.com/docs/api-reference).