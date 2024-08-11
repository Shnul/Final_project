# Final_project

### Prerequisites

- Docker
- Docker Compose
- Python 3.x
- Virtual Environment (optional but recommended)

### Setup

1. **Clone the repository:**

    ```sh
    git clone https://github.com/Shnul/Final_project.git
    cd Final_project
    ```

2. **Set up the virtual environment:**

    ```sh
    python -m venv .venv
    source .venv/bin/activate  # On Windows use `.venv\Scripts\activate`
    ```

3. **Install the dependencies:**

    ```sh
    pip install -r requirements.txt
    ```

4. **Start the Docker containers:**

    ```sh
    docker-compose up -d
    ```

5. **Initialize the database:**

    ```sh
    docker exec -i <db_container_name> psql -U <username> -d <database> -f init.sql
    ```

6. **Start the Jupyter Notebook:**

    ```sh
    ./start-notebook.sh
    ```

## Usage

- **Hadoop:** Configuration files and scripts for Hadoop.
- **Kafka:** Configuration files and scripts for Kafka.
- **Spark:** Configuration files and scripts for Spark.
- **ELK:** Configuration files and scripts for the ELK stack.
- **Database:** SQL scripts for initializing and managing the database.

## Contributing

Please feel free to submit issues, fork the repository and send pull requests!

## License

This project is licensed under the MIT License - see the LICENSE file for details.
