# Hadoop vs Spark Performance Comparison

## 📊 Big Data Framework Benchmarking

**Course:** WIH3003 – Big Data Applications and Analytics  
**Institution:** Universiti Malaya, Faculty of Computer Science and Information Technology

---

## 👥 Contributors

| Name | GitHub Username |
|------|-----------------|
| Ryan Chin Jian Hwa | [@Wrynaft](https://github.com/Wrynaft) |
| Kueh Pang Lang | [@pang-lang](https://github.com/pang-lang) |

---

## 📌 Project Overview

This project compares the performance of **Apache Hadoop (MapReduce)** and **Apache Spark** by implementing and benchmarking three queries on the same dataset. The goal is to evaluate and contrast how these two big data processing frameworks handle identical workloads.

---

## 📂 Dataset

**Name:** AI Assistant Usage in Student Life 
**Source:** [Kaggle](https://www.kaggle.com/datasets/ayeshasal89/ai-assistant-usage-in-student-life-synthetic)

This synthetic dataset captures how students interact with AI assistants for various tasks in their academic and personal lives.

---

## 🔍 Queries Implemented

| Query | Description |
|-------|-------------|
| **Query 1** | Word count on the entire dataset |
| **Query 2** | Average satisfaction ratings by task type |
| **Query 3** | Count of AI usage frequency ("Use Again") by task type |

Each query was implemented in both frameworks:
- **Hadoop:** Java (MapReduce paradigm)
- **Spark:** Scala

---

## 📈 Performance Metrics

The following metrics were used to compare Hadoop and Spark:

| Metric | Description |
|--------|-------------|
| **Execution Time** | Total time taken to complete the query |
| **Heap Memory** | JVM heap memory consumption |
| **Non-Heap Memory** | JVM non-heap memory usage |
| **Throughput** | Records processed per unit time |

---

## 🏆 Key Findings

**Spark outperforms Hadoop across all queries and all metrics.**

Detailed analysis and visualizations are included in the project report.

---

## 🛠️ Technologies Used

| Framework | Language |
|-----------|----------|
| Apache Hadoop | Java |
| Apache Spark | Scala |

