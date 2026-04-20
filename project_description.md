# IKT553: Intelligent Database Management  
## Project: Data Warehousing Strategies

This project is completed in groups and documented through a written group report. Each group will also present a demo. Groups are **self-organized**, and each group must choose a **different data-intensive application domain**.

---

## 📌 Project Description

You are required to:

- Design, implement, demonstrate, and document a **Data Warehouse (DW)**  
- Consolidate data to support **efficient, data-driven decision-making**

### Application Domain
Each group must select a unique domain, for example:
- E-commerce  
- Health management  
- Epidemiological data monitoring  

You must:
- Define **decision support queries**
- Discuss them with the instructor
- Select **data sources** (real and/or simulated)
- Perform **data transformation and cleaning** where necessary

---

## 🏗️ Implementation Requirements

### 1. Relational Data Warehouse
- Design a **multidimensional model**
- Implement using a **Star Schema**

### 2. NoSQL Implementations
Provide **two alternative implementations** using NoSQL systems.

Recommended:
- **Neo4j**
- **MongoDB**

(Other systems are allowed.)

---

## 👥 Suggested Team Responsibilities

Although all members should contribute to all tasks, consider assigning primary responsibilities:

1. **Relational DW Design & Implementation**
2. **NoSQL Implementation #1**
3. **NoSQL Implementation #2**
4. **Dockerization**
   - Containerize:
     - RDBMS
     - NoSQL systems
     - Kafka (data streaming)
5. **(Optional - Extra Credit)**  
   Apply **kSQL** in your data pipeline  

---

## ⚠️ Assumptions

You may make assumptions if:
1. They are **explicitly stated** in the report  
2. They are **reasonable**  

When in doubt → ask the instructor.

---

## 📄 Final Report Requirements

Your report must include:

1. **System Overview**
   - Users, administrators, and roles

2. **Assumptions**

3. **Data Description**

4. **Data Loading Process**
   - Cleaning and transformation

5. **Star Schema Design**
   - Fact table
   - Dimension tables
   - DDL statements
   - SQL for loading data

6. **Pre-Aggregated Summary Tables**
   - DDL statements
   - SQL for creation and population
   - Batch job specification

7. **Queries & Front-End**
   - DW queries
   - Interface description

8. **User Scenarios**

9. **NoSQL Implementations**
   - Two alternative platforms

10. **Dockerization**

11. **Data Streaming Functionality**

12. **Comparison**
   - Relational vs NoSQL
   - Advantages & disadvantages

---

## 🎥 Demo Requirements

- All group members must attend
- Each member must:
  - Explain their contribution
  - Demonstrate their part
- Source code must be **available online**

---

## ❓ FAQ

### 1. Front-End Design
**Question:**  
Should we create separate UIs for MySQL, Neo4j, and MongoDB?

**Answer:**  
Use a single **“umbrella” front-end** to access all implementations.

---

### 2. Summary Tables Batch Job
**Question:**  
Should batch jobs include new, all, or changed data?

**Answer:**  
Only provide **SQL specifications** — no actual data handling required.

---

### 3. Summary Tables vs Nightly Batch
**Question:**  
Are they the same?

**Answer:**  
No.

- **Nightly Batch Job**
  - Loads Fact & Dimension tables from operational databases  

- **Summary Tables**
  - Built from Fact & Dimension tables  
  - Require a **separate batch job**

---

### 4. NoSQL Requirements
**Question:**  
Should NoSQL implementations include Star Schema and summaries?

**Answer:**  
Yes, but adapted:

- MongoDB → **Document model**
- Neo4j → **Graph model**

Goal:  
Map **relational DW concepts → NoSQL systems**

---

## ✅ Key Objective

Understand how **Data Warehousing concepts**:
- Star schema  
- Fact/dimension modeling  
- Aggregations  

can be translated into **NoSQL architectures**.

---