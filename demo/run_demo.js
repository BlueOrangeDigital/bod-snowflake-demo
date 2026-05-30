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

// ---------------------------------------------------------------------------
// Narration — sourced from VIDEO-SCRIPT.md scene scripts
// ---------------------------------------------------------------------------
const NARRATIONS = {
  splash: `Welcome! Today I'm demonstrating Snowflake AI and Cortex with a real-world use case — analyzing financial markets using both traditional machine learning and large language models.`,

  opening: `Here's what we built. We're ingesting live data from two free public sources: stock prices from Alpha Vantage, and SEC filings from the EDGAR API. All of it flows into Snowflake, where we run two AI pipelines — a traditional ML pipeline for stock price prediction, and a Cortex AI pipeline using large language models for text summarization and sentiment analysis. Let's see it in action.`,

  setup: `Let me show you how the whole environment gets spun up. We use OpenTofu to provision all the Snowflake infrastructure from code — databases, schemas, warehouses, and scheduled tasks. Once the infrastructure is ready, we install the Python dependencies and run two ingestion scripts to pull live data from Alpha Vantage and SEC EDGAR into Snowflake. Finally, we execute the ML and Cortex AI pipeline SQL files through SnowSQL to build the feature tables, run the models, and generate the Cortex outputs.`,

  dataIngestion: `Here's our stock price time series — 10 symbols, 100 days of history. And here are the SEC filings we're ingesting — 8-K forms with full text content for our large language model analysis. This is the raw material for everything that follows.`,

  mlPipeline: `Our ML pipeline starts with feature engineering. We calculate moving averages, momentum indicators, and volatility. Using these features, we trained a linear regression model on six months of historical data. Here are the predictions for the next seven days, with confidence intervals. And here's our model performance across all symbols — achieving three to five percent mean absolute percentage error. Pretty solid for a simple linear model running entirely inside Snowflake.`,

  cortexAI: `Now for the exciting part — Cortex AI. We're using Snowflake's built-in large language model functions to process unstructured text. AI Complete generates concise summaries of SEC filings directly in SQL. AI Sentiment analyzes the tone — positive, negative, or neutral. We classify each filing into categories like mergers and acquisitions, IPO, or restructuring. Here's a spotlight on M&A activity we automatically detected. And finally, we generate an executive briefing — a full natural language report summarizing all recent market activity, produced entirely by an LLM running inside Snowflake.`,

  dashboards: `Everything comes together in Snowsight dashboards. Here's our ML prediction view — forecast versus actuals for every symbol. And here's the Cortex AI dashboard with sentiment trends and classification breakdown. We can see how market sentiment shifts over time, drill into classification distribution, and explore individual filings — all without leaving Snowflake.`,

  closing: `In just a few minutes, we demonstrated live data ingestion from free public APIs, traditional ML for time series forecasting with regression, and Cortex AI for LLM-powered summarization, sentiment analysis, and classification — all running entirely within Snowflake, with zero external infrastructure. The entire setup is automated with OpenTofu, and the pipelines run on Snowflake Tasks for daily updates. All the code is on GitHub. Thanks for watching!`,
};

// Filler phrases spoken when Snowflake execution outlasts narration by 5+ seconds
const WAIT_FILLERS = [
  "Snowflake is processing the query across the full dataset...",
  "This one's doing some heavy lifting behind the scenes...",
  "You can see it running across all symbols in parallel...",
  "Bigger queries sometimes take a moment — worth the wait.",
  "Almost there...",
];
const WAIT_DONE_PHRASE = "Oh, there it is.";

// ── Narration capture state — populated by speak(), consumed by muxAudio() ──
const _narrations = [];       // { startMs, audioPath }
let   _recordingStart = null; // set once recording begins
let   _speakCounter = 0;

// speak() — speaks text via macOS `say` (audible) AND saves it to an AIFF
// file with a timestamp so we can mux the narration into the video later.
// Resolves when audible playback finishes (preserves timing).
function speak(text) {
  if (process.platform !== 'darwin') return Promise.resolve();
  const id = ++_speakCounter;
  const audioPath = `/tmp/demo_say_${process.pid}_${id}.aiff`;
  const startMs = _recordingStart === null ? 0 : Date.now() - _recordingStart;
  const arg = JSON.stringify(text);
  const { exec } = require('child_process');
  const live = new Promise((r) => exec(`say -r 155 ${arg}`).on('close', r));
  const file = new Promise((r) => exec(`say -r 155 -o ${audioPath} ${arg}`).on('close', r));
  return Promise.all([live, file]).then(() => {
    _narrations.push({ startMs, audioPath });
  });
}

// muxAudio() — combine the captured narration AIFF files with the video.
// Each clip is delayed to its startMs offset and mixed into a single track,
// then muxed with the silent .webm into a new file with "-audio" suffix.
async function muxAudio(videoPath) {
  if (!videoPath || _narrations.length === 0) return null;
  const { execFileSync } = require('child_process');
  const fs = require('fs');
  const path = require('path');

  // Drop any clips whose audio file is missing or empty
  const clips = _narrations.filter((n) => {
    try { return fs.statSync(n.audioPath).size > 0; } catch { return false; }
  });
  if (clips.length === 0) return null;

  const dir = path.dirname(videoPath);
  const base = path.basename(videoPath, path.extname(videoPath));
  const outPath = path.join(dir, `${base}-audio.webm`);

  // Build ffmpeg args: input video + each AIFF; filter_complex applies
  // adelay per clip then amix into a single track.
  const args = ['-y', '-i', videoPath];
  for (const c of clips) args.push('-i', c.audioPath);

  const filterParts = clips.map((c, i) => `[${i + 1}:a]adelay=${c.startMs}|${c.startMs}[a${i}]`);
  const mixInputs = clips.map((_, i) => `[a${i}]`).join('');
  const filter = `${filterParts.join(';')};${mixInputs}amix=inputs=${clips.length}:duration=longest:normalize=0[aout]`;

  args.push(
    '-filter_complex', filter,
    '-map', '0:v',
    '-map', '[aout]',
    '-c:v', 'copy',
    '-c:a', 'libopus',
    '-b:a', '128k',
    outPath,
  );

  console.log(`\n  🔊 Muxing ${clips.length} narration clips into video…`);
  try {
    execFileSync('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
    // Cleanup AIFF temp files
    for (const c of clips) { try { fs.unlinkSync(c.audioPath); } catch {} }
    return outPath;
  } catch (err) {
    console.warn(`  ⚠️  ffmpeg mux failed: ${err.message}`);
    return null;
  }
}

// Queries from the demo script
const DEMO_STEPS = [
  {
    part: 'Part 2 — Data Ingestion',
    chapterTitle: 'Live Market Data',
    chapterSubtitle: 'Stocks & SEC Filings in Snowflake',
    narration: NARRATIONS.dataIngestion,
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
    chapterTitle: 'Predicting Markets',
    chapterSubtitle: 'ML Regression at Scale',
    narration: NARRATIONS.mlPipeline,
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
    chapterTitle: 'LLMs in SQL',
    chapterSubtitle: 'Cortex AI: Summarize, Analyze, Classify',
    narration: NARRATIONS.cortexAI,
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
    chapterTitle: 'Intelligence at a Glance',
    chapterSubtitle: 'Live AI Dashboards in Snowsight',
    narration: NARRATIONS.dashboards,
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

// Terminal commands for the setup & ingestion scene
const TERMINAL_SCENE = {
  part: 'Part 1 — Infrastructure & Ingestion Setup',
  chapterTitle: 'Infrastructure as Code',
  chapterSubtitle: 'OpenTofu · Python · Snowflake',
  narration: NARRATIONS.setup,
  groups: [
    {
      label: 'OpenTofu — provision Snowflake infrastructure',
      commands: [
        {
          cmd: 'tofu init',
          output: [
            'Initializing the backend...',
            'Initializing provider plugins...',
            '- Finding Snowflake-Labs/snowflake versions matching "~> 0.95"...',
            '- Installing Snowflake-Labs/snowflake v0.95.0...',
            '- Installing hashicorp/aws v5.31.0...',
            '',
            'OpenTofu has been successfully initialized!',
          ].join('\n'),
        },
        {
          cmd: 'tofu plan',
          output: [
            'OpenTofu will perform the following actions:',
            '',
            '  # snowflake_database.demo will be created',
            '  # snowflake_schema.raw_data will be created',
            '  # snowflake_schema.ml_models will be created',
            '  # snowflake_schema.cortex_ai will be created',
            '  # snowflake_schema.dashboards will be created',
            '  # snowflake_warehouse.ingestion_wh will be created',
            '  # snowflake_warehouse.ml_wh will be created',
            '  # snowflake_warehouse.cortex_wh will be created',
            '  # snowflake_table.stock_prices will be created',
            '  # snowflake_table.real_estate will be created',
            '  # snowflake_table.sec_filings will be created',
            '',
            'Plan: 12 to add, 0 to change, 0 to destroy.',
          ].join('\n'),
        },
        {
          cmd: 'tofu apply -auto-approve',
          output: [
            'snowflake_database.demo: Creating...',
            'snowflake_database.demo: Creation complete after 1s',
            'snowflake_schema.raw_data: Creating...',
            'snowflake_schema.ml_models: Creating...',
            'snowflake_schema.cortex_ai: Creating...',
            'snowflake_schema.dashboards: Creating...',
            'snowflake_warehouse.ingestion_wh: Creation complete after 2s',
            'snowflake_warehouse.ml_wh: Creation complete after 2s',
            'snowflake_warehouse.cortex_wh: Creation complete after 2s',
            'snowflake_table.stock_prices: Creation complete after 1s',
            'snowflake_table.real_estate: Creation complete after 1s',
            'snowflake_table.sec_filings: Creation complete after 1s',
            '',
            'Apply complete! Resources: 12 added, 0 changed, 0 destroyed.',
          ].join('\n'),
        },
      ],
    },
    {
      label: 'Python — install dependencies',
      commands: [
        {
          cmd: 'pip install -r requirements.txt',
          output: [
            'Collecting snowflake-connector-python==3.12.0',
            'Collecting snowflake-snowpark-python==1.21.0',
            'Collecting scikit-learn==1.4.0',
            'Collecting xgboost==2.0.3',
            'Collecting pandas==2.1.4',
            'Collecting numpy==1.26.3',
            'Installing collected packages: numpy, pandas, scikit-learn, xgboost,',
            '    snowflake-connector-python, snowflake-snowpark-python',
            'Successfully installed 24 packages',
          ].join('\n'),
        },
      ],
    },
    {
      label: 'Python — ingest data',
      commands: [
        {
          cmd: 'python ingest/fetch_stock_prices.py',
          output: [
            '[fetch_stock_prices] Connecting to Snowflake...',
            '[fetch_stock_prices] Fetching AAPL  (100 rows)  → loaded',
            '[fetch_stock_prices] Fetching MSFT  (100 rows)  → loaded',
            '[fetch_stock_prices] Fetching GOOGL (100 rows)  → loaded',
            '[fetch_stock_prices] Fetching AMZN  (100 rows)  → loaded',
            '[fetch_stock_prices] Fetching META  (100 rows)  → loaded',
            '[fetch_stock_prices] Fetching NVDA  (100 rows)  → loaded',
            '[fetch_stock_prices] Fetching TSLA  (100 rows)  → loaded',
            '[fetch_stock_prices] Fetching JPM   (100 rows)  → loaded',
            '[fetch_stock_prices] Fetching V     (100 rows)  → loaded',
            '[fetch_stock_prices] Fetching WMT   (100 rows)  → loaded',
            '✓ Inserted 1000 rows into RAW_DATA.STOCK_PRICES',
          ].join('\n'),
        },
        {
          cmd: 'python ingest/fetch_sec_filings.py',
          output: [
            '[fetch_sec_filings] Querying EDGAR for recent 8-K filings...',
            '[fetch_sec_filings] AAPL  → 3 filings',
            '[fetch_sec_filings] MSFT  → 2 filings',
            '[fetch_sec_filings] GOOGL → 4 filings',
            '[fetch_sec_filings] AMZN  → 3 filings',
            '[fetch_sec_filings] META  → 2 filings',
            '[fetch_sec_filings] NVDA  → 3 filings',
            '[fetch_sec_filings] Downloading filing text from sec.gov...',
            '✓ Inserted 25 filings into RAW_DATA.SEC_FILINGS',
          ].join('\n'),
        },
      ],
    },
    {
      label: 'SnowSQL — run pipelines',
      commands: [
        {
          cmd: 'snowsql -c demo -f sql/ml_pipeline.sql',
          output: [
            '* SnowSQL * v1.3.2',
            'Type SQL statements or !help',
            'demo#COMPUTE_WH@AI_CORTEX_DEMO.ML_MODELS>',
            '',
            'View STOCK_FEATURES successfully created.',
            'Model STOCK_PRICE_MODEL trained on 1000 rows (took 18.4s).',
            'Table STOCK_PREDICTIONS successfully created.',
            'View MODEL_PERFORMANCE successfully created.',
            'View STOCK_PREDICTION_DASHBOARD successfully created.',
            'Task REFRESH_STOCK_PREDICTIONS successfully created.',
            '',
            '✓ ML pipeline complete.',
          ].join('\n'),
        },
        {
          cmd: 'snowsql -c demo -f sql/cortex_pipeline.sql',
          output: [
            '* SnowSQL * v1.3.2',
            'demo#COMPUTE_WH@AI_CORTEX_DEMO.CORTEX_AI>',
            '',
            'Table FILING_SUMMARIES created — 25 rows summarized via AI_COMPLETE (took 22.1s).',
            'View SENTIMENT_TRENDS successfully created.',
            'View CLASSIFICATION_SUMMARY successfully created.',
            'View MA_ACTIVITY successfully created.',
            'View EXECUTIVE_BRIEFING successfully created.',
            'View CORTEX_AI_DASHBOARD successfully created.',
            'Task PROCESS_NEW_FILINGS successfully created.',
            '',
            '✓ Cortex AI pipeline complete.',
          ].join('\n'),
        },
      ],
    },
  ],
};

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

function formatChapterTime(ms) {
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  return `${String(m).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`;
}

async function showTitleCard(page, title, subtitle = '') {
  const subtitleHtml = subtitle
    ? `<div class="subtitle">${subtitle}</div>`
    : '';
  const html = `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  @keyframes fadeUp   { from { opacity:0; transform:translateY(24px); } to { opacity:1; transform:translateY(0); } }
  @keyframes shimmer  { 0%,100%{opacity:1;} 50%{opacity:.65;} }
  @keyframes scanline { 0%{background-position:0 0;} 100%{background-position:0 100%;} }
  * { margin:0; padding:0; box-sizing:border-box; }
  body {
    background: radial-gradient(ellipse at 40% 60%, #0d2b4a 0%, #06111e 55%, #020a12 100%);
    height:100vh; overflow:hidden;
    display:flex; flex-direction:column;
    align-items:center; justify-content:center;
    font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
  }
  body::after {
    content:''; position:fixed; inset:0; pointer-events:none;
    background:repeating-linear-gradient(0deg,transparent,transparent 3px,rgba(0,0,0,.07) 3px,rgba(0,0,0,.07) 4px);
  }
  .snowflake {
    font-size:64px; color:#29B5E8; margin-bottom:28px;
    animation: fadeUp .5s ease-out both, shimmer 3s 1s ease-in-out infinite;
  }
  .title {
    font-size:46px; font-weight:700; color:#ffffff;
    text-align:center; letter-spacing:-.02em; line-height:1.15;
    max-width:860px; padding:0 48px;
    animation: fadeUp .6s .15s ease-out both;
    text-shadow: 0 0 60px rgba(41,181,232,.35);
  }
  .subtitle {
    font-size:18px; font-weight:500; color:#29B5E8;
    text-align:center; margin-top:22px;
    letter-spacing:.12em; text-transform:uppercase;
    animation: fadeUp .6s .35s ease-out both;
  }
  .rule {
    width:72px; height:2px; margin-top:32px;
    background:linear-gradient(90deg,transparent,#29B5E8,transparent);
    animation: fadeUp .6s .5s ease-out both;
  }
</style>
</head>
<body>
  <div class="snowflake">❄</div>
  <div class="title">${title}</div>
  ${subtitleHtml}
  <div class="rule"></div>
</body>
</html>`;

  await page.goto(`data:text/html,${encodeURIComponent(html)}`, { waitUntil: 'domcontentloaded' });
  await pause(3200);
}

const BANNER_STYLE = `
  position: fixed;
  top: 0; left: 0; right: 0;
  z-index: 2147483647;
  background: linear-gradient(90deg, #29B5E8 0%, #1A6FA8 100%);
  color: #fff;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  font-size: 18px;
  font-weight: 600;
  letter-spacing: 0.02em;
  padding: 12px 24px;
  display: flex;
  align-items: center;
  gap: 14px;
  box-shadow: 0 2px 12px rgba(0,0,0,0.25);
  pointer-events: none;
`;

async function showBanner(page, part, label) {
  const partText  = part  ? `${part}` : '';
  const labelText = label ? `  ›  ${label}` : '';
  await page.evaluate(({ partText, labelText, style }) => {
    let el = document.getElementById('__demo_banner__');
    if (!el) {
      el = document.createElement('div');
      el.id = '__demo_banner__';
      el.setAttribute('style', style);

      const logo = document.createElement('span');
      logo.textContent = '❄';
      logo.style.cssText = 'font-size:22px; flex-shrink:0;';
      el.appendChild(logo);

      const text = document.createElement('span');
      text.id = '__demo_banner_text__';
      el.appendChild(text);

      document.body.prepend(el);
    }
    document.getElementById('__demo_banner_text__').textContent = partText + labelText;
  }, { partText, labelText, style: BANNER_STYLE });
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

/**
 * Render an animated terminal in the browser and type each command + output.
 * Captured by the Playwright video recorder.
 */
async function runTerminalScene(page, scene) {
  console.log(`\n${'─'.repeat(50)}`);
  console.log(`  ${scene.part}`);
  console.log('─'.repeat(50));

  // Build a full-page terminal HTML shell
  const terminalHtml = `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    background: #1e1e2e;
    color: #cdd6f4;
    font-family: 'SF Mono', 'Cascadia Code', 'Fira Code', 'Courier New', monospace;
    font-size: 15px;
    line-height: 1.6;
    padding: 0;
    height: 100vh;
    display: flex;
    flex-direction: column;
  }
  .titlebar {
    background: #313244;
    padding: 10px 20px;
    display: flex;
    align-items: center;
    gap: 8px;
    flex-shrink: 0;
  }
  .dot { width: 13px; height: 13px; border-radius: 50%; }
  .dot.red   { background: #f38ba8; }
  .dot.yellow{ background: #f9e2af; }
  .dot.green { background: #a6e3a1; }
  .title { margin-left: 12px; color: #bac2de; font-size: 13px; }
  .terminal {
    flex: 1;
    padding: 20px 28px;
    overflow-y: auto;
  }
  .group-label {
    color: #89b4fa;
    font-size: 13px;
    font-weight: bold;
    margin: 18px 0 8px;
    text-transform: uppercase;
    letter-spacing: 0.08em;
  }
  .prompt { color: #a6e3a1; }
  .command { color: #cdd6f4; }
  .output { color: #6c7086; white-space: pre; padding-left: 2px; }
  .blank  { height: 1.4em; }
  .cursor {
    display: inline-block;
    width: 9px;
    height: 16px;
    background: #cdd6f4;
    vertical-align: text-bottom;
    animation: blink 1s step-start infinite;
  }
  @keyframes blink { 50% { opacity: 0; } }
</style>
</head>
<body>
  <div class="titlebar">
    <div class="dot red"></div>
    <div class="dot yellow"></div>
    <div class="dot green"></div>
    <span class="title">zsh — ~/bod-snowflake-demo</span>
  </div>
  <div class="terminal" id="term"></div>
</body>
</html>`;

  await page.goto(`data:text/html,${encodeURIComponent(terminalHtml)}`, { waitUntil: 'domcontentloaded' });
  await showBanner(page, scene.part, '');
  await pause(800);

  for (const group of scene.groups) {
    console.log(`\n  ▶ ${group.label}`);

    // Write group label
    await page.evaluate((label) => {
      const term = document.getElementById('term');
      const el = document.createElement('div');
      el.className = 'group-label';
      el.textContent = `# ${label}`;
      term.appendChild(el);
    }, group.label);

    await showBanner(page, scene.part, group.label);
    await pause(600);

    for (const { cmd, output } of group.commands) {
      // Type the command character by character
      const lineEl = await page.evaluateHandle(() => {
        const term = document.getElementById('term');
        const line = document.createElement('div');
        const prompt = document.createElement('span');
        prompt.className = 'prompt';
        prompt.textContent = '$ ';
        const cmdSpan = document.createElement('span');
        cmdSpan.className = 'command';
        cmdSpan.id = '__current_cmd__';
        const cursor = document.createElement('span');
        cursor.className = 'cursor';
        cursor.id = '__cursor__';
        line.appendChild(prompt);
        line.appendChild(cmdSpan);
        line.appendChild(cursor);
        term.appendChild(line);
        term.scrollTop = term.scrollHeight;
        return cmdSpan;
      });

      // Animate typing
      for (const char of cmd) {
        await page.evaluate(({ char }) => {
          const el = document.getElementById('__current_cmd__');
          if (el) el.textContent += char;
          const term = document.getElementById('term');
          if (term) term.scrollTop = term.scrollHeight;
        }, { char });
        await pause(28 + Math.random() * 30);
      }

      // Remove cursor + insert a blank line (simulating Enter)
      await page.evaluate(() => {
        const term = document.getElementById('term');
        const c = document.getElementById('__cursor__');
        if (c) c.remove();
        const blank = document.createElement('div');
        blank.className = 'blank';
        term.appendChild(blank);
        term.scrollTop = term.scrollHeight;
      });
      await pause(500);

      // Show output, then insert a trailing blank line before next command
      await page.evaluate((outputText) => {
        const term = document.getElementById('term');
        const out = document.createElement('div');
        out.className = 'output';
        out.textContent = outputText;
        term.appendChild(out);
        const blank = document.createElement('div');
        blank.className = 'blank';
        term.appendChild(blank);
        term.scrollTop = term.scrollHeight;
      }, output);

      await pause(1200);
    }
  }

  // Final blinking cursor at prompt
  await page.evaluate(() => {
    const term = document.getElementById('term');
    const line = document.createElement('div');
    const prompt = document.createElement('span');
    prompt.className = 'prompt';
    prompt.textContent = '$ ';
    const cursor = document.createElement('span');
    cursor.className = 'cursor';
    line.appendChild(prompt);
    line.appendChild(cursor);
    term.appendChild(line);
    term.scrollTop = term.scrollHeight;
  });

  await pause(2500);
  console.log('  ✓ Terminal scene complete');
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

  const videoDir = `${__dirname}/../recordings`;
  require('fs').mkdirSync(videoDir, { recursive: true });

  // ── Phase 1: Login in a non-recording context ────────────────────────────
  // Playwright can't pause recording mid-session, so we do login in a
  // throwaway context, save its session state, then start the real
  // recording with that state already loaded — login never hits the video.
  console.log('  🔐 Logging in (not recorded)…');
  const loginContext = await browser.newContext({
    viewport: { width: 1600, height: 800 },
  });
  const loginPage = await loginContext.newPage();
  attachOAuthHandler(loginPage);
  await loginPage.goto(START_URL, { waitUntil: 'domcontentloaded' });

  const loginPath = `app.snowflake.com/${SNOWFLAKE_ORG}/${SNOWFLAKE_ACCOUNT}`;
  const loginDeadline = Date.now() + 300_000;
  while (true) {
    const current = loginPage.url();
    if (current.includes(loginPath.toLowerCase()) && current.includes('#/')) break;
    if (Date.now() > loginDeadline) {
      console.error('  ✗ Timed out waiting for login');
      await browser.close();
      process.exit(1);
    }
    await pause(1000);
  }
  await pause(1000);
  console.log('  ✓ Logged in!');

  // Save session state and close the login context
  const storageState = await loginContext.storageState();
  await loginContext.close();

  // ── Phase 2: Start recording with saved session — already logged in ──────
  console.log('  🎬 Starting recording (logged-in session reused)…');
  const context = await browser.newContext({
    viewport: { width: 1600, height: 800 },
    recordVideo: { dir: videoDir, size: { width: 1600, height: 800 } },
    storageState,
  });
  const page = await context.newPage();
  // Mark recording start so speak() can compute audio offsets accurately
  _recordingStart = Date.now();

  // ── Splash title + welcome narration — very first frame of the recording ─
  await Promise.all([
    showTitleCard(page, 'AI Prediction in Snowflake!', 'Powered by Snowflake Cortex & Snowpark ML'),
    speak(NARRATIONS.splash),
  ]);

  // Chapter tracking — timestamps relative to recording start
  const chapters = [];
  const recordChapter = (title, subtitle) => {
    const elapsed = Date.now() - _recordingStart;
    chapters.push({ time: elapsed, title, subtitle });
    console.log(`  📍 Chapter: [${formatChapterTime(elapsed)}] ${title}`);
  };

  // ── Opening chapter card + architecture narration ────────────────────────
  recordChapter('Snowflake AI & Cortex', 'Financial Intelligence Demo');
  await showTitleCard(page, 'Snowflake AI & Cortex', 'Financial Intelligence Demo');

  // DEBUG: dump screenshot + body HTML so we can identify real selectors
  await page.goto(START_URL, { waitUntil: 'domcontentloaded' });
  await pause(300);
  await showBanner(page, 'Snowflake AI & Cortex Demo', '');
  await page.screenshot({ path: '/tmp/snowsight-homepage.png', fullPage: false });
  const bodyHTML = await page.evaluate(() => document.body.innerHTML);
  require('fs').writeFileSync('/tmp/snowsight-homepage.html', bodyHTML);
  console.log('  🔍 Debug snapshot saved: /tmp/snowsight-homepage.png + .html');

  // Speak architecture overview; linger on Snowflake homepage until done
  await speak(NARRATIONS.opening);
  await pause(1000);

  // ── Setup & ingestion scene ───────────────────────────────────────────────
  console.log('\nStep 2/3: Running setup & ingestion scene…\n');
  recordChapter(TERMINAL_SCENE.chapterTitle, TERMINAL_SCENE.chapterSubtitle);
  await showTitleCard(page, TERMINAL_SCENE.chapterTitle, TERMINAL_SCENE.chapterSubtitle);
  // Run terminal animation and narration concurrently; linger until both done
  await Promise.all([
    runTerminalScene(page, TERMINAL_SCENE),
    speak(TERMINAL_SCENE.narration),
  ]);
  await pause(1000);

  // ── SQL demo scenes ───────────────────────────────────────────────────────
  console.log('\nStep 3/3: Running demo queries…\n');
  for (const part of DEMO_STEPS) {
    console.log(`\n${'─'.repeat(50)}`);
    console.log(`  ${part.part}`);
    console.log('─'.repeat(50));

    recordChapter(part.chapterTitle, part.chapterSubtitle);
    await showTitleCard(page, part.chapterTitle, part.chapterSubtitle);

    // Navigate back to Snowflake after title card
    await page.goto(START_URL, { waitUntil: 'domcontentloaded' });
    await pause(1500);

    // Start narration concurrent with query execution
    let queriesDone = false;
    const speechPromise = speak(part.narration);

    // Filler watcher: kicks in if execution outlasts narration by 5+ seconds
    const fillerPromise = (async () => {
      await speechPromise;
      if (queriesDone) return;
      await pause(5000);
      if (queriesDone) return;
      let i = 0;
      let usedFiller = false;
      while (!queriesDone) {
        await speak(WAIT_FILLERS[i++ % WAIT_FILLERS.length]);
        usedFiller = true;
        if (!queriesDone) await pause(1500);
      }
      if (usedFiller) await speak(WAIT_DONE_PHRASE);
    })();

    for (const step of part.steps) {
      await showBanner(page, part.part, step.label);
      await runQuery(context, page, step);
    }
    queriesDone = true;

    // Wait for narration + any fillers to finish before moving on
    await fillerPromise;
    await pause(1000);
  }

  // ── Closing title card + closing narration ────────────────────────────────
  recordChapter('AI-Powered Finance', 'Entirely in Snowflake');
  const closingSpeech = speak(NARRATIONS.closing);
  await showTitleCard(page, 'AI-Powered Finance', 'Entirely in Snowflake');

  // ── Write chapters file ───────────────────────────────────────────────────
  // YouTube requires the first chapter to be at 00:00. If our earliest
  // chapter starts later, prepend an "Intro" chapter so YouTube parses
  // the list at all.
  const chaptersForFile = chapters.slice();
  if (chaptersForFile.length === 0 || chaptersForFile[0].time > 0) {
    chaptersForFile.unshift({ time: 0, title: 'Intro', subtitle: '' });
  }
  const chaptersText = chaptersForFile
    .map(({ time, title, subtitle }) =>
      `${formatChapterTime(time)} ${title}${subtitle ? ': ' + subtitle : ''}`)
    .join('\n');
  const chaptersPath = `${__dirname}/../recordings/chapters.txt`;
  require('fs').mkdirSync(`${__dirname}/../recordings`, { recursive: true });
  require('fs').writeFileSync(chaptersPath, chaptersText + '\n');
  console.log(`\n  📋 Chapters file written: ${chaptersPath}`);
  console.log(chaptersText);

  // Linger on closing card until narration finishes
  await closingSpeech;

  console.log('\n' + '='.repeat(60));
  console.log('  ✅ Demo complete!');
  console.log('='.repeat(60));

  // Close automatically. Set DEMO_HOLD_OPEN=1 to inspect the browser before close.
  if (process.env.DEMO_HOLD_OPEN === '1') {
    await waitForEnter('\n  → Press ENTER to close the browser: ');
  } else {
    console.log('\n  ⏳ Closing browser in 2s (set DEMO_HOLD_OPEN=1 to wait for ENTER)…');
    await pause(2000);
  }
  const videoPath = await page.video()?.path();
  await browser.close();
  if (videoPath) {
    console.log(`\n  🎬 Silent recording saved: ${videoPath}`);
    const muxed = await muxAudio(videoPath);
    if (muxed) console.log(`  🎬 Final recording (with audio): ${muxed}`);
  }
})();
