# 🏨 Hotel Booking Data Engineering Project

## 📌 Summary

This project focuses on building a data engineering pipeline that extracts, processes, and stores hotel booking data for analytical purposes. It is designed to demonstrate core data engineering skills including workflow orchestration, data transformation, and data warehousing using industry-standard tools.

---

## 🎯 Objectives

- Extract hotel booking data from various sources.
- Transform and clean data to ensure consistency and quality.
- Load the processed data into a PostgreSQL database.
- Automate the data pipeline using Apache Airflow.
- Containerize the entire project with Docker for easy deployment and scalability.

---

## 🔄 System Flow

1. **Data Extraction**: Retrieve raw hotel booking data from CSV, API, or simulated sources.
2. **Data Transformation**: Clean and preprocess the data using Python scripts (e.g., handle missing values, normalize formats).
3. **Load Processed Data**: Insert the transformed data into a PostgreSQL database.
4. **Orchestration**: Apache Airflow is used to schedule and monitor the ETL pipeline.
5. **Containerization**: Docker ensures all components run in isolated and reproducible environments.


---

## 🛠 Technology Used

- **Apache Airflow** – for pipeline orchestration and scheduling.
- **Docker** – for containerized deployment of all components.
- **Python** – for data extraction, transformation, and scripting.
- **PostgreSQL** – as the target data warehouse for storing cleaned data.

---

## ✅ Conclusion

This project successfully demonstrates how to build and deploy a scalable and automated ETL pipeline for hotel booking data. With a clear separation of tasks, orchestration using Airflow, and containerization with Docker, the system is robust and ready for extension into real-world data processing workflows.
