# Digital Banking System — Microservices


## Services Overview

| Service | Port | Responsibility |
|---|---|---|
| api-gateway | 8080 | Single entry point, Rate limiting |
| account-service | 8081 | Account management, Balance |
| transaction-service | 8082 | Money transfers, Transaction history |
| payment-service | 8083 | Razorpay integration, Webhooks |
| fraud-detection-service | 8084 | Real time fraud detection via Redis |
| notification-service | 8085 | Transaction and fraud alerts |

---

## Architecture Flow

<img width="915" height="484" alt="final" src="https://github.com/user-attachments/assets/f079b205-b808-49c5-bc44-e7672a9cd3c2" />


## Kafka Topics

| Topic | Publisher | Consumer |
|---|---|---|
| transaction.initiated | Transaction Service | Fraud Detection |
| fraud.check.result | Fraud Detection | Transaction Service |
| transaction.completed | Transaction Service | Account Service, Notification |
| fraud.detected | Fraud Detection | Account Service, Notification |
| payment.completed | Payment Service | Notification |

---
