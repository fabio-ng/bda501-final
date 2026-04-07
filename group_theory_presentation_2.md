# Group Presentation 2: Big Data System Design

**vubc - fsb**

## Assignment Description

In this assignment, each group has selected a **real-world use case**. Your task is to design a **scalable Big Data system architecture**.

**Important:** You are **not required** to use any specific technology (e.g., Spark, Hadoop, Kafka). Instead, you must **choose appropriate tools, libraries, and frameworks** based on your problem.

The goal is to demonstrate your ability to:

- Analyze a real-world problem
- Design a data processing pipeline
- Select suitable technologies and justify your choices

## 1. Requirement 1: Problem Formalization (1.5 marks)

Clearly define your problem:

- Describe the **input data** (fields, types, format, source)
- Define the **expected output** (prediction, alert, insight, visualization)
- Specify the **processing type**: batch, streaming, or hybrid

## 2. Requirement 2: Architecture Design (3.5 marks)

Design a complete system architecture from data ingestion to output.

```
Data Source → Ingestion → Storage → Processing → Output
```

Your design must include:

### Data Ingestion

- How data is collected (API, sensors, logs, files, etc.)
- Tool choice (e.g., Kafka, Flume, manual ingestion, etc.)

### Storage Layer

- Data storage solution (HDFS, database, data lake, etc.)
- Data format (CSV, JSON, Parquet, etc.)

### Processing Layer

- Processing framework (e.g., Spark, Hadoop MapReduce, Flink, etc.)
- Type of computation (ML, graph analysis, aggregation, ETL)

### Output Layer

- Output destination (dashboard, database, API, alert system)

## 3. Requirement 3: Data Flow Explanation (1.5 marks)

Explain how data flows through your system:

- Describe each step from input to output
- Explain transformations and intermediate stages

**Example:**

- Sensor → Ingestion → Storage → Processing → Output

## 4. Requirement 4: Technology Selection and Justification (1.5 marks)

Justify your design choices:

- Why did you choose specific tools/frameworks?
- What are the advantages of your choices?
- Why are alternative approaches less suitable?

## 5. Requirement 5: Data Modeling (1.0 mark)

Define your data representation:

- Schema or structure of the data
- Feature design (if machine learning is used)

## 6. Requirement 6: Scalability and Optimization (1.0 mark)

Discuss how your system handles large-scale data:

- How does your system scale (horizontal/vertical)?
- Partitioning or parallel processing strategy
- Trade-off between latency and throughput

**Guiding Question:**

- What happens if data volume increases by 10x?

## 7. Bonus (Optional +1.0 marks)

- Provide pseudo-code or workflow for your system
- Propose advanced techniques (ML models, graph algorithms, optimization strategies)

## Grading Rubric

| Criteria                     | Marks |
| ---------------------------- | ----- |
| Problem Formalization        | 1.5   |
| Architecture Design          | 3.5   |
| Data Flow Explanation        | 1.5   |
| Technology Justification     | 1.5   |
| Data Modeling                | 1.0   |
| Scalability & Optimization   | 1.0   |
| **Total**                    | **10.0** |
| Bonus                        | +1.0  |

## Submission Requirements

- A report (PDF) including all sections above
- A clear system architecture diagram (required)
- Maximum 10 pages

### Important Notes:

- There is no single correct answer; evaluation is based on **reasoning and justification**
- Poorly justified technology choices will be penalized
- Designs without clear data flow or scalability consideration will receive low marks
