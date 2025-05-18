# 🌦️ Real-Time Air-Quality Data Pipeline with Visualization Project
## Subject: DSI321 Big Data Infrastructure
Prepared by: Lapatrada Truktrongkij

## 📌 **Project Overview**
This project is developed as part of the DSI321: BIG DATA INFRASTRUCTURE course, aiming to build a real-time air quality monitoring system focused on PM2.5 levels across Thailand. The system collects data from the [Air4Thai](http://air4thai.pcd.go.th), processes it through an automated pipeline orchestrated by Prefect, and presents insights through an interactive dashboard built with Streamlit. Key technologies include Docker for containerization, LakeFS for data version control, and Typhoon AI (a large language model) for generating human-readable summaries and insights. The goal is to make PM2.5 data both accessible and actionable for the general public and decision-makers.

### 📖 **Introduction**
Air pollution, particularly fine particulate matter (PM2.5), has emerged as a major environmental and public health concern in Thailand. Prolonged exposure to PM2.5 can lead to serious respiratory and cardiovascular issues. Real-time monitoring of PM2.5 levels is therefore crucial for raising public awareness and informing timely responses.

To support this need, the project implements a fully automated data pipeline that collects PM2.5 data from the **Air4Thai** network—an official source managed by the **Pollution Control Department of Thailand**. The system is designed for scalability, reproducibility, and ease of use by integrating modern data engineering tools.

The pipeline leverages [Prefect.io](https://www.prefect.io/) for task scheduling and orchestration, ensuring robust handling of data fetching, cleaning, and transformation. All components are containerized with [Docker](https://www.docker.com/) to maintain consistency across environments, and [LakeFS](https://lakefs.io/) is used for managing dataset versioning and lifecycle.

To make the output data more user-friendly and insightful, [Typhoon AI](https://opentyphoon.ai/), a large language model, is integrated into the system. It automatically generates natural language summaries of air quality trends, detects anomalies, and provides regional highlights bridging the gap between raw data and human interpretation.

The final output is an interactive web-based dashboard built with [Streamlit](https://streamlit.io/) that allows users to explore real-time PM2.5 data and receive AI-powered insights, making the system valuable for both public users and environmental analysts.

### 🛠️ Key Components

- **Data Collection**: Automated retrieval of hourly PM2.5 data from the Air4Thai API.

- **Data Processing**: Cleaning, transforming, and storing data using Python.

- **Workflow Orchestration**: Prefect handles task scheduling, dependencies, retries, and logging.

- **Containerization**: Docker and Docker Compose manage consistent, modular environments.

- **Data Versioning**: LakeFS tracks and controls dataset changes for reproducibility.

- **Natural Language Summarization**: Typhoon AI generates easy-to-understand air quality summaries.

- **Interactive Dashboard**: Streamlit visualizes real-time PM2.5 data alongside LLM-generated insights.
  
### 🕹️ Technologies Used

- **Python**: Backend development, data handling, and orchestration scripts.

- **Prefect**: Automates the flow of tasks within the data pipeline.

- **Docker & Docker Compose**: For consistent multi-container deployments.

- **LakeFS**: Git-like version control for data lifecycle management.

- **Typhoon AI (LLM)**: Provides AI-generated natural language summaries.

- **Streamlit**: Frontend dashboard for real-time, interactive data visualization.
  
### 🎯 Expected Outcomes

- A fully automated, real-time PM2.5 monitoring pipeline.

- Interactive Streamlit dashboard for public and expert users.

- AI-powered natural language summaries to enhance data interpretation.

- Reliable, scalable, and version-controlled system architecture.

## Installation

1. Clone the repository:
   ```bash
   $ git clone <this-repo-url>
   $ cd <this-repo-folder>
   $ docker compose up -d --build

