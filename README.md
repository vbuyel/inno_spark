# InnoSpark - PySpark Data Processing Pipeline

A modular, high-performance Apache Spark (PySpark) processing framework designed for batch ETL workloads and analytical queries against the Pagila sample PostgreSQL database.

---

## 1. Architectural Overview

The project follows a **modular, object-oriented ETL pipeline architecture**. Analytical tasks inherit from a centralized base template (`SparkTask`) that encapsulates the `SparkSession` lifecycle, PostgreSQL connectivity via JDBC, table ingestion into distributed DataFrames, and standardized JSON output export.

```mermaid
flowchart TD
    subgraph Storage & External Data
        PG[(PostgreSQL Database\nPagila)]
        ENV[".env Configuration"]
    end

    subgraph Core Framework ["practice/"]
        PC["PostgresConnector\n(db_connection.py)"]
        ST["SparkTask Base Class\n(general_cls.py)"]
        MAIN["Orchestrator Entrypoint\n(main.py)"]
    end

    subgraph Tasks ["practice/tasks/"]
        T1["SparkTask1\n(Movie count by category)"]
        T2["SparkTask2\n(Top 10 actors by total rentals)"]
        T3["SparkTask3\n(Top spending movie category)"]
        T4["SparkTask4\n(Movies absent from inventory)"]
        T5["SparkTask5\n(Top 3 actors in 'Children' movies)"]
        T6["SparkTask6\n(Active vs inactive customers per city)"]
        T7["SparkTask7\n(Top category by rental hours for cities starting with 'a')"]
    end

    subgraph Output [".results/"]
        OUT1["task1/\n(JSON)"]
        OUT2["task2/\n(JSON)"]
        OUT3["task3/\n(JSON)"]
        OUT4["task4/\n(JSON)"]
        OUT5["task5/\n(JSON)"]
        OUT6["task6/\n(JSON)"]
        OUT7["task7/\n(JSON)"]
    end

    ENV --> PC
    ENV --> ST
    PC -->|Credentials & Connection| ST
    ST -->|Inherits & Reuses| T1
    ST -->|Inherits & Reuses| T2
    ST -->|Inherits & Reuses| T3
    ST -->|Inherits & Reuses| T4
    ST -->|Inherits & Reuses| T5
    ST -->|Inherits & Reuses| T6
    ST -->|Inherits & Reuses| T7

    MAIN -->|Instantiates & Runs| T1
    MAIN -->|Instantiates & Runs| T2
    MAIN -->|Instantiates & Runs| T3
    MAIN -->|Instantiates & Runs| T4
    MAIN -->|Instantiates & Runs| T5
    MAIN -->|Instantiates & Runs| T6
    MAIN -->|Instantiates & Runs| T7

    PG -->|JDBC Read\norg.postgresql:postgresql:42.7.3| ST
    T1 -->|Transform & Write| OUT1
    T2 -->|Transform & Write| OUT2
    T3 -->|Transform & Write| OUT3
    T4 -->|Transform & Write| OUT4
    T5 -->|Transform & Write| OUT5
    T6 -->|Transform & Write| OUT6
    T7 -->|Transform & Write| OUT7
```

---

## 2. Core Architectural Components

### 2.1. Application Entrypoint & Orchestration (`practice/main.py`)
- **Centralized Batch Runner**: Sequentially instantiates and executes analytical tasks (`SparkTask1` through `SparkTask7`).
- **Unified Logging**: Sets up standard Python `logging` (`%(asctime)s [%(levelname)s] [%(name)s]: %(message)s`) at root entrypoint to ensure uniform log emission across all execution stages.
- **Resource Safety & Resilience**: Wraps the entire workflow in a `try...except...finally` block. Every instantiated task triggers `close_all()` in the `finally` block, ensuring clean teardown of both `SparkSession` resources and PostgreSQL connection handles regardless of execution outcome.

### 2.2. Base Task Abstraction (`practice/general_cls.py`)
The `SparkTask` class serves as the foundation implementing the **Template Method Pattern**:
- **SparkSession Management**: Automatically configures and retrieves an active `SparkSession` using `.getOrCreate()`, dynamically loading the PostgreSQL JDBC driver (`org.postgresql:postgresql:42.7.3`) via Apache Ivy. JVM internal logs are silenced to `WARN` level to keep logs focused and readable.
- **Database Connectivity**: Integrates with `PostgresConnector` to construct JDBC connection URLs and credentials.
- **Data Ingestion (`load_table`)**: Provides a unified method to read PostgreSQL tables directly into distributed PySpark DataFrames via JDBC.
- **Standardized Export (`json_inload`)**: Persists DataFrames to partitioned JSON files within `practice/.results/{task_name}` (or a custom target path) using overwrite mode.
- **Graceful Teardown (`close_all`)**: Handles shutdown of the `SparkSession` and the underlying database connection.
- **Execution Contract (`execute`)**: Abstract execution contract overridden by each task subclass.

### 2.3. Database Connection Layer (`practice/db_connection.py`)
- **`PostgresConnector`**: Encapsulates connection parameters (`DB_HOST`, `DB_PORT`, `DB_DATABASE`, `DB_USER`, `DB_PASSWORD`) loaded securely from `.env` via `python-dotenv`.
- Provides dedicated connection management via `psycopg2` alongside parameters needed by PySpark JDBC readers.

### 2.4. Task Implementations (`practice/tasks/`)
All seven analytical queries are implemented as dedicated modular subclasses of `SparkTask`:

| Module | Goal & Analytical Query | Key Transformations & Spark Logic | Tables Used | Output Path |
|--------|-------------------------|-----------------------------------|-------------|-------------|
| `task_1.py` | Count of movies per category, sorted descending by count then ascending by category name. | `film_category` joined with broadcasted `category`; `groupBy("name")`, `count("film_id")`, `orderBy(desc("movie_count"), col("category"))`. | `category`, `film_category` | `.results/task1` |
| `task_2.py` | Top 10 actors whose movies rented the most, sorted descending. | `rental` → `inventory` inner join, `groupBy("film_id")` to get rental count per film; broadcast join with `film_actor` and `actor`; `groupBy("first_name", "last_name")`, `sum("rental_count")`, `limit(10)`. | `rental`, `inventory`, `actor`, `film_actor` | `.results/task2` |
| `task_3.py` | Category of movies on which the most money was spent. | `payment` joined with `rental`, then broadcast-joined with `inventory`, `film_category`, `category`; `groupBy("category")`, `sum("amount")`, `orderBy(desc("total_spent"))`, `limit(1)`. | `payment`, `rental`, `inventory`, `film_category`, `category` | `.results/task3` |
| `task_4.py` | Titles of movies not present in inventory. | `left_anti` join between `film` and deduplicated `inventory` on `film_id` with `broadcast(inventory)`; selects `title`. | `film`, `inventory` | `.results/task4` |
| `task_5.py` | Top 3 actors appearing most in "Children" category movies (including all ties). | Filter `category` to `"Children"` pushed down before join; broadcast join with `film_category`, `film_actor`, and `actor`; window ranking using `dense_rank().over(orderBy(desc("movie_count")))`, filter `place <= 3`. | `category`, `film_category`, `film_actor`, `actor` | `.results/task5` |
| `task_6.py` | Cities with count of active (`active = 1`) and inactive (`active != 1`) customers, sorted descending by inactive count. | Broadcast joins on `address` and `city` with `customer`; conditional aggregations using `count(when(col("active") == 1, 1))` and `count(when(col("active") != 1, 1))`; `orderBy(desc("inactive_customers"), col("city"))`. | `address`, `city`, `customer` | `.results/task6` |
| `task_7.py` | For each city starting with "a" (case-insensitive), find the movie category with the most rental hours. Marks whether that city also contains a "-". | Cities starting with "a" LEFT JOIN cities containing "-" on `city_id`; broadcast join with `address`, `customer`, `rental`, `inventory`, `film_category`, `category`; `groupBy("city_starts_with_a", "city_with_hyphens", "name")`, `sum("rental_hours")`; `dense_rank()` partitioned by `(city_starts_with_a, city_with_hyphens)`; filter `rank == 1`; `cache()` + `unpersist()`. | `rental`, `inventory`, `film_category`, `category`, `customer`, `address`, `city` | `.results/task7` |

---

## 3. Query Optimization & Performance Engineering

All tasks have been optimized to minimize network I/O, JVM memory pressure, and cluster shuffle stages:

```mermaid
graph LR
    subgraph Optimizations ["Spark Optimization Techniques"]
        direction TB
        OPT1["Column Projection Pruning\n(Select only required columns from JDBC)"]
        OPT2["Broadcast Hash Joins\n(broadcast() on dimension tables to eliminate shuffles)"]
        OPT3["Pre-aggregation at Source Grain\n(Aggregate rental counts at film level before actor join in Task 2)"]
        OPT4["Native Left-Anti Joins\n(Short-circuit join instead of left outer + null check in Task 4)"]
        OPT5["Predicate Pushdown\n(Filter small dimensions before joins in Task 5)"]
        OPT6["DataFrame Caching & Lifecycle\n(Cache shared 7-table join, unpersist after use in Task 7)"]
        OPT7["Partitioned Window Functions\n(partitionBy in Window to avoid global single-partition shuffle in Task 7)"]
    end
```

1. **Column Projection Pruning**: All `load_table()` calls are immediately followed by `.select(...)` to pull only required columns over JDBC, reducing serialization overhead and executor memory footprint.

2. **Broadcast Hash Joins (`broadcast`)**: Small dimension tables (`category`, `actor`, `inventory`, `film_category`, `address`, `city`) are explicitly wrapped with `broadcast()`. Replaces expensive distributed `SortMergeJoin` with fast local in-memory hash lookups.

3. **Pre-aggregation at Source Grain (Task 2)**: `rental` is joined with `inventory` and aggregated by `film_id` before joining with `film_actor` and `actor`. This prevents row multiplication from the many-to-many `film_actor` relationship inflating the intermediate dataset.

4. **Native Left-Anti Joins (Task 4)**: Replaced conventional `left` join followed by `.where(col(...).isNull())` with PySpark's native `how="left_anti"` join against deduplicated inventory film IDs. Spark executes this as an optimized broadcast anti-hash join.

5. **Predicate Pushdown & Broadcast Filtering (Task 5)**: The `"Children"` category filter is pushed directly onto the 16-row `category` table before joining, broadcasting a 1-row DataFrame that prunes rows early in the pipeline.

6. **In-Memory DataFrame Caching & Lifecycle Management (Task 7)**: The 7-table joined base DataFrame is cached via `.cache()` to serve both the aggregation and ranking steps without re-evaluation, then immediately released with `.unpersist()`.

7. **Partitioned Window Functions (Task 7)**: `dense_rank()` uses `Window.partitionBy("city_starts_with_a", "city_with_hyphens")` instead of a global `Window.orderBy(...)`, which avoids collapsing all data into a single executor partition and eliminates the `WindowExec` performance warning.

---

## 4. Class Hierarchy & Design Patterns

```mermaid
classDiagram
    class PostgresConnector {
        +str host
        +int port
        +str database
        +str user
        +str password
        +Connection conn
        +connect() Connection
        +disconnect() void
    }

    class SparkTask {
        +str task_name
        +PostgresConnector db
        +SparkSession spark
        +create_spark_session() SparkSession
        +load_table(table_name: str) DataFrame
        +json_inload(df: DataFrame, path: str) void
        +close_all() void
        +execute() void
    }

    class SparkTask1 {
        +execute() void
    }
    class SparkTask2 {
        +execute() void
    }
    class SparkTask3 {
        +execute() void
    }
    class SparkTask4 {
        +execute() void
    }
    class SparkTask5 {
        +execute() void
    }
    class SparkTask6 {
        +execute() void
    }
    class SparkTask7 {
        +execute() void
    }

    SparkTask --> PostgresConnector : manages
    SparkTask1 --|> SparkTask : inherits
    SparkTask2 --|> SparkTask : inherits
    SparkTask3 --|> SparkTask : inherits
    SparkTask4 --|> SparkTask : inherits
    SparkTask5 --|> SparkTask : inherits
    SparkTask6 --|> SparkTask : inherits
    SparkTask7 --|> SparkTask : inherits
```

---

## 5. Execution Lifecycle

1. **Initialization & Setup**:
   - `practice/main.py` is invoked.
   - Root logger is initialized with timestamped formatting.
   - Tasks (`SparkTask1` through `SparkTask7`) are instantiated.
2. **Session & Connection Management**:
   - Base `SparkTask.__init__` reads database parameters via `PostgresConnector` (`.env`).
   - `SparkSession` is configured with PostgreSQL JDBC driver coordinates (`org.postgresql:postgresql:42.7.3`). Subsequent tasks reuse the existing JVM session via `SparkSession.builder.getOrCreate()`.
   - PySpark log level is adjusted to `WARN`.
3. **Execution Phase**:
   - The runner loops over `tasks` and invokes `.execute()` on each instance.
   - `load_table()` loads needed PostgreSQL relations via JDBC.
   - Transformations (joins, window functions, aggregations, broadcasts) execute lazily across Spark executors.
   - `json_inload()` writes output datasets into `practice/.results/<task_name>/`.
4. **Graceful Teardown**:
   - A `finally` block in `main.py` calls `close_all()` on each task.
   - Database connections (`psycopg2`) are closed.
   - The `SparkSession` driver and JVM gateway are cleanly terminated.

---

## 6. Directory Structure

```text
inno_spark/
├── .gitignore                 # Git ignore rules
├── requirements.txt           # Python dependencies (pyspark, psycopg2, etc.)
├── README.md                  # Architecture, task documentation & project guide
└── practice/                  # Core application package
    ├── __init__.py
    ├── main.py                # Application entrypoint & task runner
    ├── general_cls.py         # SparkTask base class (Template Method)
    ├── db_connection.py       # PostgresConnector class
    ├── .results/              # Output directory for exported JSON partitions
    │   ├── task1/             # Task 1 output: movie count by category
    │   ├── task2/             # Task 2 output: top 10 actors by total rentals
    │   ├── task3/             # Task 3 output: top spending movie category
    │   ├── task4/             # Task 4 output: movies absent from inventory
    │   ├── task5/             # Task 5 output: top 3 actors in "Children" movies
    │   ├── task6/             # Task 6 output: active/inactive customers per city
    │   └── task7/             # Task 7 output: top rental-hours category per 'a'-city
    └── tasks/                 # Individual task implementations
        ├── __init__.py        # Exposes SparkTask1 through SparkTask7
        ├── task_1.py          # Movie count by category
        ├── task_2.py          # Top 10 actors by total rentals
        ├── task_3.py          # Top spending movie category
        ├── task_4.py          # Movies absent from inventory
        ├── task_5.py          # Top 3 actors in "Children" movies (dense rank)
        ├── task_6.py          # Active and inactive customer counts per city
        └── task_7.py          # Top rental-hours category for cities starting with 'a',
                               # with hyphen flag via LEFT JOIN
```

---

## 7. Technology Stack

| Component | Technology / Version | Description |
|-----------|----------------------|-------------|
| **Compute Engine** | [Apache Spark (PySpark)](https://spark.apache.org/) `>=4.0.0` | Distributed query execution, DataFrame operations, window functions |
| **Database** | [PostgreSQL (Pagila)](https://www.postgresql.org/) | Relational DVD rental database running locally or in Docker |
| **Driver / Connector** | `org.postgresql:postgresql:42.7.3` | JDBC driver for PySpark PostgreSQL extraction |
| **Database Client** | `psycopg2-binary` `>=2.9.9` | Python PostgreSQL driver for connection management |
| **Configuration** | `python-dotenv` `>=1.0.0` | Environment variable management from `.env` |
| **Data Serialization** | `pyarrow` `>=18.0.0`, `pandas` `>=2.2.0` | Optimized columnar execution and interoperability |

---

## 8. Getting Started

### 8.1. Prerequisites
- **Java**: OpenJDK 17 or 21 (required by Apache Spark)
- **Python**: Python 3.10+
- **Docker & Docker Compose** (optional, for running the database in a container)

### 8.2. Environment Configuration
Create a `.env` file in the project root:

```ini
DB_HOST=...
DB_PORT=...
DB_DATABASE=...
DB_USER=...
DB_PASSWORD=...
```

### 8.3. Python Dependencies Installation
Create and activate a virtual environment, then install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 8.4. Running the Pipeline
Execute all analytical tasks sequentially from the project root:

```bash
python practice/main.py
```

Results will be exported as JSON files inside `practice/.results/`.

---

## 9. How to Add a New Task

To introduce an additional analytical task into the pipeline:

1. Create a new task module in `practice/tasks/task_X.py`.
2. Inherit from `SparkTask` and implement `.execute()`:
   ```python
   from pyspark.sql.functions import col, broadcast
   from general_cls import SparkTask

   class SparkTaskX(SparkTask):
       def __init__(self) -> None:
           super().__init__("taskX")

       def execute(self) -> None:
           # Apply column pruning at load time
           df = self.load_table("table_name").select("col_a", "col_b")
           # Apply transformations
           result_df = df.filter(col("col_a") > 0)
           # Persist results
           self.json_inload(result_df)
   ```
3. Export the class in `practice/tasks/__init__.py`.
4. Import and append the instance to the `tasks` array in `practice/main.py`.
