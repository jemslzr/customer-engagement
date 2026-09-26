/**
 * CustomerPulse - Confluent Cloud Account Seed Script
 * 
 * Usage:
 *   node seed_confluent_accounts.js
 * 
 * Populates the `customer_accounts` topic in Confluent Cloud so that
 * Flink SQL temporal joins can match real-time clickstream events.
 */

const fs = require('fs');
const path = require('path');

// Load .env if present
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, 'utf8');
  envContent.split('\n').forEach(line => {
    const trimmed = line.trim();
    if (trimmed && !trimmed.startsWith('#')) {
      const idx = trimmed.indexOf('=');
      if (idx !== -1) {
        const key = trimmed.slice(0, idx).trim();
        const val = trimmed.slice(idx + 1).trim();
        process.env[key] = val;
      }
    }
  });
}

const ENDPOINT = process.env.CONFLUENT_REST_ENDPOINT;
const CLUSTER_ID = process.env.CONFLUENT_CLUSTER_ID;
const API_KEY = process.env.CONFLUENT_API_KEY;
const API_SECRET = process.env.CONFLUENT_API_SECRET;

if (!ENDPOINT || !CLUSTER_ID || !API_KEY || !API_SECRET) {
  console.error('\n❌ Missing Confluent configuration in .env!');
  console.error('Please copy .env.example to .env and fill in your Confluent Cloud credentials:');
  console.error('- CONFLUENT_REST_ENDPOINT');
  console.error('- CONFLUENT_CLUSTER_ID');
  console.error('- CONFLUENT_API_KEY');
  console.error('- CONFLUENT_API_SECRET\n');
  process.exit(1);
}

const sampleAccounts = [
  { user_id: 'user_seed_1', account_name: 'Acme Global Corp (Sarah Connor)', tier: 'Enterprise', mrr: 1999.0, email: 'sarah@acmeglobal.com' },
  { user_id: 'user_seed_2', account_name: 'David Miller Media', tier: 'Growth', mrr: 499.0, email: 'david@millermedia.io' },
  { user_id: 'user_seed_3', account_name: 'Nordic Logistics AB (Elena Rostova)', tier: 'Enterprise', mrr: 1999.0, email: 'elena@nordiclog.se' },
  { user_id: 'user_seed_4', account_name: 'Kenji Sato Labs', tier: 'Growth', mrr: 499.0, email: 'kenji@satolabs.jp' },
  { user_id: 'user_seed_5', account_name: 'Mendez Creative (Carlos Mendez)', tier: 'Starter', mrr: 99.0, email: 'carlos@mendezcreative.com' },
  { user_id: 'user_seed_6', account_name: 'Al-Mansoor Trading (Amira Al-Mansoor)', tier: 'Growth', mrr: 499.0, email: 'amira@almansoor.ae' },
  { user_id: 'user_shopper_demo', account_name: 'Interactive Test Account', tier: 'Enterprise', mrr: 1999.0, email: 'demo@customerpulse.io' }
];

async function seed() {
  const url = `${ENDPOINT.replace(/\/$/, '')}/kafka/v3/clusters/${CLUSTER_ID}/topics/customer_accounts/records`;
  const auth = Buffer.from(`${API_KEY}:${API_SECRET}`).toString('base64');

  console.log(`\n🚀 Seeding ${sampleAccounts.length} customer accounts to Confluent Cloud topic: customer_accounts...`);
  console.log(`Endpoint: ${url}\n`);

  for (const acct of sampleAccounts) {
    const payload = {
      key: { type: 'JSON', data: acct.user_id },
      value: { type: 'JSON', data: acct }
    };

    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: {
          'Authorization': `Basic ${auth}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      });

      if (res.ok) {
        console.log(`✅ [SEEDED] ${acct.user_id} -> ${acct.account_name} (${acct.tier}, $${acct.mrr}/mo)`);
      } else {
        const text = await res.text();
        console.error(`⚠️ Failed to seed ${acct.user_id} (Status ${res.status}): ${text}`);
      }
    } catch (err) {
      console.error(`❌ Network error sending ${acct.user_id}:`, err.message);
    }
  }

  console.log('\n✨ Seeding completed! Check your topic in Confluent Cloud UI -> Topics -> customer_accounts -> Messages.\n');
}

seed();
