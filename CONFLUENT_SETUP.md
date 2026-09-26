# Confluent Cloud Integration Guide & Dev Day Submission

This document contains everything needed to submit your application for the **Developer Day Macbook Pro Award ("Most Impactful App")** and step-by-step instructions to set up the live Confluent Cloud backend.

---

## 📋 Dev Day Form Submission Cheat Sheet

### 1. App Description (What it does, whom it's for, what the benefit is)
> **CustomerPulse** is an open-source real-time customer friction and churn prevention platform designed for online businesses, SaaS founders, and e-commerce product teams. Rather than relying on delayed 24-48 hour batch analytics when customers have already left, CustomerPulse captures high-velocity user clickstreams and session events in Confluent Cloud.
>
> Using Apache Flink SQL, CustomerPulse continuously calculates a real-time **Customer Friction Score (CFS)** by aggregating behavioral distress signals (payment failures, checkout hesitation, navigation loops, and rage clicks) in sliding stream windows. It then performs a **temporal table join** with customer account profiles and subscription tiers to calculate the exact **Real-Time Revenue at Risk ($)** and automatically route personalized retention interventions (VIP Slack alerts, rescue discount promo codes, concierge chat) before the visitor abandons their session.
>
> **Business Impact:** Recovers thousands of dollars in lost Monthly Recurring Revenue (MRR) and abandoned cart value by reducing churn at the exact moment of friction with sub-second streaming response times.

### 2. Which Confluent connector(s) are you using?
> 1. **Confluent HTTP / Webhook Sink Connector (`retention_interventions`)** — Streams real-time rescue triggers and churn interventions to external business endpoints (Slack, Discord, Klaviyo, SendGrid, or internal webhooks).
> 2. **Datagen Source Connector or PostgreSQL CDC Source (`customer_accounts`)** — Streams live customer account tiers, MRR, and profile metadata into Apache Kafka with Avro serialization registered in Confluent Schema Registry.

### 3. Paste here your schema
```json
{
  "type": "record",
  "name": "RetentionIntervention",
  "namespace": "io.customerpulse.retention",
  "doc": "Enriched real-time churn intervention event joining behavioral friction with customer account tiers",
  "fields": [
    { "name": "session_id", "type": "string", "doc": "Unique visitor browser session ID" },
    { "name": "user_id", "type": "string", "doc": "Customer account identifier" },
    { "name": "account_name", "type": "string", "doc": "Company or customer display name" },
    { "name": "tier", "type": "string", "doc": "Subscription tier: Enterprise, Growth, or Starter" },
    { "name": "mrr", "type": "double", "doc": "Monthly recurring revenue value in USD" },
    { "name": "friction_score", "type": "long", "doc": "Calculated real-time friction intensity score (0-100)" },
    { "name": "exposed_cart_value", "type": "double", "doc": "Value of items currently in cart" },
    { "name": "last_active_page", "type": "string", "doc": "URL path where friction occurred" },
    { "name": "calculated_at", "type": "string", "doc": "Window timestamp of evaluation" },
    { "name": "recommended_action", "type": "string", "doc": "Automated playbook: VIP_CSM_SLACK_ALERT, RESCUE_PROMO_15PCT, or PROACTIVE_CHAT_OFFER" }
  ]
}
```

### 4. Stream Lineage Screenshot
> In your Confluent Cloud environment, click on **Stream Lineage** from the left menu. Take a screenshot showing `customer_clickstream` + `customer_accounts` flowing into `enriched_clickstream` &rarr; `retention_interventions` (Sink). Upload it to Google Drive / imgbb.com and paste the link in your form.

---

## 🛠️ Step-by-Step Confluent Cloud Setup Guide

### Step 1: Create Flink SQL Tables
In Confluent Cloud, open **Flink** &rarr; select your Compute Pool &rarr; open **SQL Workspace**.

Run each statement sequentially:

#### 1. Dimension Table: Customer Accounts
```sql
CREATE TABLE customer_accounts (
  user_id STRING NOT NULL,
  account_name STRING,
  tier STRING,
  mrr DOUBLE,
  email STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
);
```

#### 2. Seed Customer Accounts (Native SQL)
```sql
INSERT INTO customer_accounts (user_id, account_name, tier, mrr, email) VALUES
  ('user_seed_1', 'Acme Global Corp (Sarah Connor)', 'Enterprise', 1999.0, 'sarah@acmeglobal.com'),
  ('user_seed_2', 'David Miller Media', 'Growth', 499.0, 'david@millermedia.io'),
  ('user_seed_3', 'Nordic Logistics AB (Elena Rostova)', 'Enterprise', 1999.0, 'elena@nordiclog.se'),
  ('user_seed_4', 'Kenji Sato Labs', 'Growth', 499.0, 'kenji@satolabs.jp'),
  ('user_seed_5', 'Mendez Creative (Carlos Mendez)', 'Starter', 99.0, 'carlos@mendezcreative.com'),
  ('user_seed_6', 'Al-Mansoor Trading (Amira Al-Mansoor)', 'Growth', 499.0, 'amira@almansoor.ae');
```

#### 3. Ingestion Table: Real-Time Clickstream
```sql
CREATE TABLE customer_clickstream (
  session_id STRING,
  user_id STRING,
  event_id STRING,
  event_type STRING,
  page_path STRING,
  cart_value DOUBLE,
  region STRING,
  duration_ms INT
);
```

#### 4. Real-Time Temporal Enrichment Stream
```sql
CREATE TABLE enriched_clickstream AS
SELECT
  c.session_id,
  c.user_id,
  a.account_name,
  a.tier,
  a.mrr,
  c.event_type,
  c.page_path,
  c.cart_value
FROM customer_clickstream c
JOIN customer_accounts FOR SYSTEM_TIME AS OF c.`$rowtime` a
  ON c.user_id = a.user_id;
```

#### 5. Real-Time Intervention Dispatch (Windowed Friction Aggregation)
```sql
CREATE TABLE retention_interventions AS
SELECT
  session_id,
  user_id,
  account_name,
  tier,
  mrr,
  COUNT(*) AS total_interactions,
  SUM(CASE
    WHEN event_type = 'payment_failed' THEN 45
    WHEN event_type = 'checkout_abandon' THEN 45
    WHEN event_type = 'rage_click' THEN 25
    WHEN event_type = 'support_query' THEN 20
    WHEN event_type = 'checkout_start' AND cart_value > 500 THEN 15
    ELSE 1
  END) AS friction_score,
  MAX(cart_value) AS exposed_cart_value,
  MAX(page_path) AS last_active_page,
  window_end AS calculated_at,
  CASE
    WHEN tier = 'Enterprise' AND SUM(CASE WHEN event_type IN ('payment_failed', 'checkout_abandon') THEN 45 ELSE 1 END) >= 40 THEN 'VIP_CSM_SLACK_ALERT'
    WHEN MAX(cart_value) >= 1000 THEN 'RESCUE_PROMO_15PCT'
    ELSE 'PROACTIVE_CHAT_OFFER'
  END AS recommended_action
FROM TABLE(
  TUMBLE(TABLE enriched_clickstream, DESCRIPTOR(`$rowtime`), INTERVAL '1' MINUTE)
)
GROUP BY session_id, user_id, account_name, tier, mrr, window_start, window_end
HAVING SUM(CASE
  WHEN event_type = 'payment_failed' THEN 45
  WHEN event_type = 'checkout_abandon' THEN 45
  WHEN event_type = 'rage_click' THEN 25
  WHEN event_type = 'support_query' THEN 20
  WHEN event_type = 'checkout_start' AND cart_value > 500 THEN 15
  ELSE 1
END) >= 30;
```

---

### Step 2: Seed Test Clickstream Events
Run this in SQL Workspace to immediately trigger end-to-end processing:
```sql
INSERT INTO customer_clickstream (session_id, user_id, event_id, event_type, page_path, cart_value, region, duration_ms) VALUES
  ('sess_demo_1', 'user_seed_1', 'evt_101', 'checkout_abandon', '/checkout', 1250.0, 'Region_1', 4500),
  ('sess_demo_2', 'user_seed_2', 'evt_102', 'payment_failed', '/billing', 499.0, 'Region_2', 1200),
  ('sess_demo_1', 'user_seed_1', 'evt_103', 'support_query', '/support', 1250.0, 'Region_1', 6000);
```

---

### Step 3: Capture Stream Lineage Screenshot
1. In the left navigation of Confluent Cloud, click **Stream Lineage**.
2. Observe the complete pipeline graph:
   ```text
   customer_accounts ────┐
                         ├──► enriched_clickstream ──► retention_interventions
   customer_clickstream ─┘
   ```
3. Capture a clear screenshot for your submission form.
