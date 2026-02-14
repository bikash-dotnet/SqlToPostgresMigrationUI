🚀 Project Overview

A high-performance .NET application for migrating databases from SQL Server to PostgreSQL with real-time monitoring, parallel processing, and data validation capabilities.

✨ Key Features

⚡ High Performance	 --> Uses SqlClient for reading + PostgreSQL COPY for bulk inserts

🔄 Parallel Migration --> Migrate multiple tables simultaneously

📊 Real-time Dashboard --> Blazor Server UI with live progress updates

✅ Data Validation --> Row count & checksum verification

🛡️ Transaction Safety --> Each table migration is transactional

🔄 Resume Support --> State persistence for interrupted migrations

🔧 Schema Migration --> Tables, indexes, PK/FK, sequences

📈 Progress Tracking --> ETA, speed metrics, detailed logs



🛠️ Technology Stack

.NET 7/8 - Core framework

Blazor Server - Dashboard UI

SignalR - Real-time updates

SqlClient - SQL Server data access

Npgsql - PostgreSQL data access

Chart.js - Progress visualization
