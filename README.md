# 🌦️ Real-Time Air-Quality Data Pipeline with Visualization Project
## Subject: DSI321 Big Data Infrastructure
Prepared by: Lapatrada Truktrongkij

## Overview

This project is designed to build a system for monitoring and analyzing air quality in real time. By collecting data from [Air4Thai](http://air4thai.pcd.go.th), processing it, and visualizing it through an interactive dashboard, this system aims to provide valuable insights into air pollution levels, offering users the ability to monitor air quality and take necessary actions.

### **Key Components**
- **Data Collection**: Fetching air quality data from reliable public APIs.
- **Data Processing**: Cleaning, transforming, and storing data for analysis and reporting.
- **Data Visualization**: Displaying processed data through graphs, tables, and charts on a dashboard for users to interact with and make decisions.
  
### **Technologies Used**
- **Programming Language**: Python for backend services and data analysis.
- **Data Storage**: PostgreSQL for structured and persistent storage of air quality data.
- **Data Visualization**: Jupyter Notebooks, along with Python libraries like Matplotlib and Seaborn for displaying data visually.
  
- **Containerization and Orchestration**:
  - **Docker**: Used to containerize the application and ensure consistent environments across different systems.
  - **Docker Compose**: Manages multi-container setups, such as databases and Prefect orchestration services.
  
- **Workflow Management**:
  - **Prefect**: A tool for orchestrating and automating workflows. It ensures that tasks such as data fetching, processing, and visualization run smoothly with retry mechanisms, logging, and task dependencies.
  - **Prefect Server and Worker**: These components work together to schedule, monitor, and execute the data pipeline processes.

- **Version Control and Data Lake**:
  - **LakeFS**: A data version control tool that helps manage the lifecycle of datasets by storing them in a versioned and reproducible manner. LakeFS integrates well with the data pipeline and ensures that all data modifications are versioned, allowing for easy tracking and rollback of data changes.
  
### **Expected Outcomes**
- **Real-time monitoring** of air quality parameters like PM2.5, CO2, temperature, and humidity.
- **Data visualizations** on an interactive dashboard.
- **Automated data workflows** powered by Prefect for data fetching, cleaning, and processing.
- **Scalable, modular architecture** built with Docker, Prefect, and LakeFS to ensure reliability, reproducibility, and version control for all data.

## Installation

1. Clone the repository:
   ```bash
   $ git clone <this-repo-url>
   $ cd <this-repo-folder>
   $ docker compose up -d --build

