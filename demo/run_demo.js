#!/usr/bin/env node
/**
 * Snowflake AI & Cortex Demo Automation
 * Automates the Demo Script from README.md
 *
 * Usage:
 *   SNOWFLAKE_ACCOUNT=<your-account> node demo/run_demo.js
 *
 * If SNOWFLAKE_ACCOUNT is not set, the script opens app.snowflake.com
 * and lets you navigate to your account manually before logging in.
 */

const { chromium } = require('playwright');
const readline = require('readline');

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const SNOWFLAKE_ORG     = process.env.SNOWFLAKE_ORGANIZATION_NAME;
const SNOWFLAKE_ACCOUNT = process.env.SNOWFLAKE_ACCOUNT_NAME;
const SNOWFLAKE_USER    = process.env.SNOWFLAKE_USER;
const SNOWFLAKE_PASSWORD = process.env.SNOWFLAKE_PASSWORD;

if (!SNOWFLAKE_ORG || !SNOWFLAKE_ACCOUNT) {
  console.error('Error: SNOWFLAKE_ORGANIZATION_NAME and SNOWFLAKE_ACCOUNT_NAME env vars are required.');
  console.error('  source .env  (or set them manually)');
  process.exit(1);
}

const START_URL    = `https://app.snowflake.com/${SNOWFLAKE_ORG}/${SNOWFLAKE_ACCOUNT}/`;
const WORKSPACES_URL = `https://app.snowflake.com/${SNOWFLAKE_ORG}/${SNOWFLAKE_ACCOUNT}/#/workspaces`;

// Database / schema from env vars (with sane defaults matching .env-example)
const DB_RAW        = process.env.DEMO_DATABASE_RAW_DATA          || 'AI_CORTEX_DEMO';
const SCHEMA_RAW    = process.env.DEMO_SCHEMA_RAW                 || 'RAW_DATA';
const DB_ML         = process.env.DEMO_DATABASE_AI_CORTEX_DEMO    || 'AI_CORTEX_DEMO';
const SCHEMA_ML     = process.env.DEMO_SCHEMA_ML_MODELS           || 'ML_MODELS';
const DB_CORTEX     = process.env.DEMO_DATABASE_CORTEX_AI         || 'AI_CORTEX_DEMO';
const SCHEMA_CORTEX = process.env.DEMO_SCHEMA_CORTEX_AI           || 'CORTEX_AI';
// Dashboards live in the same database as ML (no separate env var needed)
const SCHEMA_DASH   = 'DASHBOARDS';

// Queries from the demo script
const DEMO_STEPS = [
  {
    part: 'Part 2 — Data Ingestion',
    steps: [
      {
        label: 'Stock Prices — RAW DATA',
        database: DB_RAW,
        schema: SCHEMA_RAW,
        sql: `SELECT * FROM ${SCHEMA_RAW}.STOCK_PRICES LIMIT 10;`,
      },
      {
        label: 'SEC Filings — RAW DATA',
        database: DB_RAW,
        schema: SCHEMA_RAW,
        sql: `SELECT COMPANY_NAME, FILING_DATE, SUBSTR(FILING_TEXT, 1, 200) FROM ${SCHEMA_RAW}.SEC_FILINGS LIMIT 5;`,
      },
    ],
  },
  {
    part: 'Part 3 — Traditional ML Pipeline',
    steps: [
      {
        label: 'AAPL Stock Features — ML Pipeline',
        database: DB_ML,
        schema: SCHEMA_ML,
        sql: `SELECT * FROM ${SCHEMA_ML}.STOCK_FEATURES WHERE SYMBOL = 'AAPL' ORDER BY DATE DESC LIMIT 10;`,
      },
      {
        label: 'AAPL Stock Predictions — ML Pipeline',
        database: DB_ML,
        schema: SCHEMA_ML,
        sql: `SELECT * FROM ${SCHEMA_ML}.STOCK_PREDICTIONS WHERE SYMBOL = 'AAPL' ORDER BY PREDICTION_DATE;`,
      },
      {
        label: 'Model Performance MAPE — ML Pipeline',
        database: DB_ML,
        schema: SCHEMA_ML,
        sql: `SELECT SYMBOL, MAPE FROM ${SCHEMA_ML}.MODEL_PERFORMANCE GROUP BY SYMBOL ORDER BY MAPE;`,
      },
    ],
  },
  {
    part: 'Part 4 — Cortex AI Pipeline',
    steps: [
      {
        label: 'Filing Summaries AI Complete — Cortex AI',
        database: DB_CORTEX,
        schema: SCHEMA_CORTEX,
        sql: `SELECT COMPANY_NAME, AI_SUMMARY FROM ${SCHEMA_CORTEX}.FILING_SUMMARIES LIMIT 3;`,
      },
      {
        label: 'Sentiment Analysis — Cortex AI',
        database: DB_CORTEX,
        schema: SCHEMA_CORTEX,
        sql: `SELECT COMPANY_NAME, SENTIMENT, SENTIMENT_SCORE FROM ${SCHEMA_CORTEX}.FILING_SUMMARIES LIMIT 5;`,
      },
      {
        label: 'Classification Breakdown — Cortex AI',
        database: DB_CORTEX,
        schema: SCHEMA_CORTEX,
        sql: `SELECT CLASSIFICATION, COUNT(*) AS COUNT FROM ${SCHEMA_CORTEX}.FILING_SUMMARIES GROUP BY CLASSIFICATION;`,
      },
      {
        label: 'MA Activity — Cortex AI',
        database: DB_CORTEX,
        schema: SCHEMA_CORTEX,
        sql: `SELECT * FROM ${SCHEMA_CORTEX}.MA_ACTIVITY LIMIT 5;`,
      },
      {
        label: 'Executive Briefing — Cortex AI',
        database: DB_CORTEX,
        schema: SCHEMA_CORTEX,
        sql: `SELECT * FROM ${SCHEMA_CORTEX}.EXECUTIVE_BRIEFING;`,
      },
    ],
  },
  {
    part: 'Part 5 — Dashboards',
    steps: [
      {
        label: 'Stock Prediction Dashboard',
        database: DB_ML,
        schema: SCHEMA_DASH,
        sql: `SELECT * FROM ${SCHEMA_DASH}.STOCK_PREDICTION_DASHBOARD LIMIT 50;`,
      },
      {
        label: 'Cortex AI Dashboard',
        database: DB_CORTEX,
        schema: SCHEMA_DASH,
        sql: `SELECT * FROM ${SCHEMA_DASH}.CORTEX_AI_DASHBOARD LIMIT 50;`,
      },
    ],
  },
];

const OAUTH_URL_PATTERN = /https:\/\/.*\.snowflakecomputing\.com\/oauth\/authorize/;

/**
 * Attach a listener to a page that auto-fills credentials on any
 * Snowflake OAuth authorize page navigated to by that page.
 */
function attachOAuthHandler(page) {
  const handler = async (frame) => {
    if (frame !== page.mainFrame()) return;
    const url = frame.url();
    if (!OAUTH_URL_PATTERN.test(url)) return;
    if (!SNOWFLAKE_USER || !SNOWFLAKE_PASSWORD) {
      console.warn('  ⚠️  OAuth page detected but SNOWFLAKE_USER / SNOWFLAKE_PASSWORD not set — skipping auto-login');
      page.off('framenavigated', handler);
      return;
    }
    console.log('  🔐 OAuth page detected — filling credentials…');
    try {
      // Wait for the username field to appear
      const userSel = 'input[name="login_name"], input[name="username"], input[type="text"][autocomplete*="user" i], input[id*="user" i], input[placeholder*="user" i]';
      await page.waitForSelector(userSel, { timeout: 10_000 });
      await page.fill(userSel, SNOWFLAKE_USER);

      const passSel = 'input[type="password"]';
      await page.waitForSelector(passSel, { timeout: 5_000 });
      await page.fill(passSel, SNOWFLAKE_PASSWORD);

      // Click the submit / login button
      const submitSel = 'button[type="submit"], input[type="submit"], button:has-text("Log In"), button:has-text("Login"), button:has-text("Sign in")';
      const submitBtn = page.locator(submitSel).first();
      await submitBtn.waitFor({ state: 'visible', timeout: 5_000 });
      await submitBtn.click();
      console.log('  ✓ OAuth credentials submitted');
    } catch (err) {
      console.warn(`  ⚠️  OAuth auto-login failed: ${err.message}`);
    } finally {
      page.off('framenavigated', handler);
    }
  };
  page.on('framenavigated', handler);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function pause(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function waitForEnter(prompt) {
  return new Promise((resolve) => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    rl.question(prompt, () => {
      rl.close();
      resolve();
    });
  });
}

let _editorDebugDone = false;

/**
 * Find the CodeMirror / ACE editor, clear it, type the SQL, and run it.
 */
async function runQuery(context, page, step) {
  const { label, database, schema, sql } = step;
  console.log(`\n  ▶ ${label}`);

  await page.getByRole('link', { name: 'Projects' }).click();
  await page.frameLocator('[title="Workspaces"]').locator("#add-new-menu-button").click();
  await page.frameLocator('[title="Workspaces"]').getByRole('menuitem', { name: 'SQL file' }).click();
  await page.keyboard.insertText(label)
  await page.keyboard.press('Enter');

  // DEBUG: on first query, dump the worksheet editor HTML + screenshot
  if (!_editorDebugDone) {
    _editorDebugDone = true;
    const html = await page.evaluate(() => document.body.innerHTML);
    require('fs').writeFileSync('/tmp/snowsight-worksheet.html', html);
    await page.screenshot({ path: '/tmp/snowsight-worksheet.png', fullPage: false });
    console.log('    🔍 Worksheet debug snapshot saved: /tmp/snowsight-worksheet.png + .html');
  }

  // ── Locate the SQL editor ────────────────────────────────────────────────
  const editorSelectors = [
    '.cm-activeLine.cm-line'
  ];

  let editor = null;
  for (const sel of editorSelectors) {
    for (let i = 0; i < 30; i++) {
      const el = await page.frameLocator('[title="Workspaces"]').locator(sel);
      if (await el.isVisible({ timeout: 30000 }).catch(() => false)) {
        editor = el;
        break;
      }
      pause(1000)
    }
  }

  if (!editor) {
    console.error('    ✗ SQL editor not found — skipping this query');
    return;
  }

  // Build the full SQL with database/schema context
  const fullSql = `USE DATABASE ${database};\nUSE SCHEMA ${schema};\n\n${sql}`;

  // Click to focus, select all, delete, then type the SQL
  await editor.click();
  await pause(300);
  await page.keyboard.press('Meta+A');
  await pause(200);
  await page.keyboard.press('Backspace');
  await pause(200);

  // Type the full SQL (with USE DATABASE/SCHEMA context) into the editor
  await page.keyboard.type(fullSql, { delay: 15 });
  await pause(500);

  const runDropDown = await page.frameLocator('[title="Workspaces"]').locator('button[aria-label="Run options"]').first()
  await runDropDown.click()
  await page.frameLocator('[title="Workspaces"]').locator('div[data-action-name="RunAll"]').click()


  // ── Wait for results ─────────────────────────────────────────────────────
  console.log('    ⏳ Waiting for results…');
  try {
    await page.frameLocator('[title="Workspaces"]').getByRole('button', { name: 'Table' }).isVisible({ timeout: 60_000 })
    console.log('    ✓ Results loaded');
  } catch {
    console.warn('    ⚠️  Results selector timed out — results may still be loading');
  }

  // Hold on results so a screen recording can capture them
  await pause(3000);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
(async () => {
  console.log('='.repeat(60));
  console.log('  Snowflake AI & Cortex Demo Automation');
  console.log('='.repeat(60));
  console.log(`\nOpening: ${START_URL}`);
  console.log('\nStep 1/2: Log in to Snowflake in the browser window.');

  const browser = await chromium.launch({
    headless: false,
    // args: ['--start-maximized'],
  });

  // const context = await browser.newContext({ viewport: null });
  const context = await browser.newContext({ viewport:{ width: 1600, height: 800 }});
  const page = await context.newPage();
  attachOAuthHandler(page);
  await page.goto(START_URL, { waitUntil: 'domcontentloaded' });

  // Poll until the URL contains the org/account path (login complete)
  console.log('  ⏳ Waiting for login…');
  const loginPath = `app.snowflake.com/${SNOWFLAKE_ORG}/${SNOWFLAKE_ACCOUNT}`;
  const loginDeadline = Date.now() + 300_000;
  while (true) {
    const current = page.url();
    // Require the SPA hash (#/) — the bare start URL also contains loginPath
    // but the hash only appears after a successful login redirect
    if (current.includes(loginPath.toLowerCase()) && current.includes('#/')) break;
    if (Date.now() > loginDeadline) {
      console.error('  ✗ Timed out waiting for login');
      await browser.close();
      process.exit(1);
    }
    await pause(1000);
  }
  await pause(2000);
  console.log('  ✓ Logged in!');

  // DEBUG: dump screenshot + body HTML so we can identify real selectors
  await page.screenshot({ path: '/tmp/snowsight-homepage.png', fullPage: false });
  const bodyHTML = await page.evaluate(() => document.body.innerHTML);
  require('fs').writeFileSync('/tmp/snowsight-homepage.html', bodyHTML);
  console.log('  🔍 Debug snapshot saved: /tmp/snowsight-homepage.png + .html');

  console.log('\nStep 2/2: Running demo queries…\n');
  for (const part of DEMO_STEPS) {
    console.log(`\n${'─'.repeat(50)}`);
    console.log(`  ${part.part}`);
    console.log('─'.repeat(50));

    for (const step of part.steps) {
      await runQuery(context, page, step);
    }
    // break; -- uncomment for just 1 table
  }

  console.log('\n' + '='.repeat(60));
  console.log('  ✅ Demo complete!');
  console.log('='.repeat(60));

  await waitForEnter('\n  → Press ENTER to close the browser: ');
  await browser.close();
})();
