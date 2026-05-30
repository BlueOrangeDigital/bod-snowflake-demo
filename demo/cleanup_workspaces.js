#!/usr/bin/env node
/**
 * Snowflake Workspace Cleanup
 * ---------------------------------------------------------------------------
 * Deletes the SQL files created by demo/run_demo.js from "My Workspace" in
 * Snowsight. Uses Playwright to drive the UI since workspace files live in
 * user state, not as schema objects (so they can't be dropped via SQL).
 *
 * Usage:
 *   source .env && node demo/cleanup_workspaces.js --yes
 *
 * Requires the same env vars as demo/run_demo.js (SNOWFLAKE_ORGANIZATION_NAME,
 * SNOWFLAKE_ACCOUNT_NAME, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD).
 */

const { chromium } = require('playwright');

const SNOWFLAKE_ORG      = process.env.SNOWFLAKE_ORGANIZATION_NAME;
const SNOWFLAKE_ACCOUNT  = process.env.SNOWFLAKE_ACCOUNT_NAME;
const SNOWFLAKE_USER     = process.env.SNOWFLAKE_USER;
const SNOWFLAKE_PASSWORD = process.env.SNOWFLAKE_PASSWORD;

if (!SNOWFLAKE_ORG || !SNOWFLAKE_ACCOUNT) {
  console.error('Error: SNOWFLAKE_ORGANIZATION_NAME and SNOWFLAKE_ACCOUNT_NAME env vars are required.');
  process.exit(1);
}
if (!process.argv.includes('--yes') && !process.argv.includes('-y')) {
  console.error(`
⚠️  This will DELETE all demo-created SQL files from your Snowsight workspace.
   Re-run with --yes to confirm:
     node demo/cleanup_workspaces.js --yes
`);
  process.exit(1);
}

const START_URL = `https://app.snowflake.com/${SNOWFLAKE_ORG}/${SNOWFLAKE_ACCOUNT}/`;

// File labels created by demo/run_demo.js (keep in sync with DEMO_STEPS)
const DEMO_FILES = [
  'Stock Prices — RAW DATA',
  'SEC Filings — RAW DATA',
  'AAPL Stock Features — ML Pipeline',
  'AAPL Stock Predictions — ML Pipeline',
  'Model Performance MAPE — ML Pipeline',
  'Filing Summaries AI Complete — Cortex AI',
  'Sentiment Analysis — Cortex AI',
  'Classification Breakdown — Cortex AI',
  'MA Activity — Cortex AI',
  'Executive Briefing — Cortex AI',
  'Stock Prediction Dashboard',
  'Cortex AI Dashboard',
];

const OAUTH_URL_PATTERN = /https:\/\/.*\.snowflakecomputing\.com\/oauth\/authorize/;

function pause(ms) { return new Promise((r) => setTimeout(r, ms)); }

function attachOAuthHandler(page) {
  const handler = async (frame) => {
    if (frame !== page.mainFrame()) return;
    if (!OAUTH_URL_PATTERN.test(frame.url())) return;
    if (!SNOWFLAKE_USER || !SNOWFLAKE_PASSWORD) {
      console.warn('  ⚠️  OAuth page detected but SNOWFLAKE_USER / SNOWFLAKE_PASSWORD not set');
      page.off('framenavigated', handler);
      return;
    }
    console.log('  🔐 OAuth page detected — filling credentials…');
    try {
      const userSel = 'input[name="login_name"], input[name="username"], input[type="text"][autocomplete*="user" i]';
      await page.waitForSelector(userSel, { timeout: 10_000 });
      await page.fill(userSel, SNOWFLAKE_USER);
      await page.fill('input[type="password"]', SNOWFLAKE_PASSWORD);
      const submit = page.locator('button[type="submit"], button:has-text("Log In"), button:has-text("Sign in")').first();
      await submit.click();
      console.log('  ✓ Credentials submitted');
    } catch (err) {
      console.warn(`  ⚠️  OAuth auto-login failed: ${err.message}`);
    } finally {
      page.off('framenavigated', handler);
    }
  };
  page.on('framenavigated', handler);
}

/**
 * Attempt to delete a single workspace file by name. Returns true on success,
 * false if the file wasn't found or deletion failed.
 *
 * Snowsight workspace delete flow (best-effort selectors):
 *   1. Hover/click the file in the left tree
 *   2. Click the overflow (kebab) menu next to it
 *   3. Click "Delete" / "Move to Trash"
 *   4. Confirm in the dialog
 */
async function deleteWorkspaceFile(page, label) {
  const workspace = page.frameLocator('[title="Workspaces"]');
  console.log(`\n  🗑  Deleting: ${label}`);

  // Locate the tree row for this file. Snowsight wraps file entries in
  // various roles depending on version — try a few common patterns.
  const candidates = [
    workspace.getByRole('treeitem', { name: label }),
    workspace.getByRole('button', { name: label }),
    workspace.getByRole('link', { name: label }),
    workspace.locator(`[role="treeitem"]:has-text(${JSON.stringify(label)})`),
    workspace.locator(`text=${JSON.stringify(label)}`).first(),
  ];

  let row = null;
  for (const c of candidates) {
    if (await c.first().isVisible({ timeout: 1500 }).catch(() => false)) {
      row = c.first();
      break;
    }
  }
  if (!row) {
    console.log(`    ⏭  not found (already deleted?)`);
    return false;
  }

  // Right-click to open the context menu (most reliable on Snowsight)
  try {
    await row.click({ button: 'right', timeout: 5000 });
    await pause(400);

    const deleteItem = workspace.getByRole('menuitem', { name: /delete|move to trash|remove/i }).first();
    if (!(await deleteItem.isVisible({ timeout: 3000 }).catch(() => false))) {
      // Close the context menu by pressing Escape, then try the overflow button approach
      await page.keyboard.press('Escape');
      await pause(300);

      await row.hover();
      const overflow = row.locator('xpath=..').locator('button[aria-label*="more" i], button[aria-label*="options" i], button[aria-label*="menu" i]').first();
      if (await overflow.isVisible({ timeout: 2000 }).catch(() => false)) {
        await overflow.click();
        await pause(400);
      } else {
        console.log(`    ✗ couldn't find delete option in context or overflow menu`);
        return false;
      }
    }

    const del = workspace.getByRole('menuitem', { name: /delete|move to trash|remove/i }).first();
    await del.click({ timeout: 3000 });
    await pause(500);

    // Confirm the delete dialog (could be in main frame or workspace frame)
    const confirmButtons = [
      page.getByRole('button', { name: /^delete$|^move to trash$|^remove$|^confirm$/i }).first(),
      workspace.getByRole('button', { name: /^delete$|^move to trash$|^remove$|^confirm$/i }).first(),
    ];
    for (const btn of confirmButtons) {
      if (await btn.isVisible({ timeout: 2000 }).catch(() => false)) {
        await btn.click();
        break;
      }
    }
    await pause(800);
    console.log(`    ✓ deleted`);
    return true;
  } catch (err) {
    console.log(`    ✗ delete failed: ${err.message}`);
    return false;
  }
}

(async () => {
  console.log('='.repeat(60));
  console.log('  Snowflake Workspace Cleanup');
  console.log('='.repeat(60));

  const browser = await chromium.launch({ headless: false });
  const context = await browser.newContext({ viewport: { width: 1600, height: 900 } });
  const page = await context.newPage();
  attachOAuthHandler(page);

  console.log(`\n  Opening: ${START_URL}`);
  await page.goto(START_URL, { waitUntil: 'domcontentloaded' });

  // Wait for login to complete
  const loginPath = `app.snowflake.com/${SNOWFLAKE_ORG}/${SNOWFLAKE_ACCOUNT}`.toLowerCase();
  const deadline = Date.now() + 300_000;
  while (true) {
    const url = page.url();
    if (url.toLowerCase().includes(loginPath) && url.includes('#/')) break;
    if (Date.now() > deadline) {
      console.error('  ✗ Timed out waiting for login');
      await browser.close();
      process.exit(1);
    }
    await pause(1000);
  }
  await pause(1500);
  console.log('  ✓ Logged in');

  // Navigate to Projects → Workspaces
  await page.getByRole('link', { name: 'Projects' }).click();
  await pause(1500);

  // Dump a debug snapshot so we can iterate on selectors if needed
  await page.screenshot({ path: '/tmp/snowsight-workspaces-cleanup.png', fullPage: false });
  const html = await page.evaluate(() => document.body.innerHTML);
  require('fs').writeFileSync('/tmp/snowsight-workspaces-cleanup.html', html);
  console.log('  🔍 Debug snapshot: /tmp/snowsight-workspaces-cleanup.png + .html');

  let deleted = 0;
  let missing = 0;
  let failed = 0;
  for (const label of DEMO_FILES) {
    const ok = await deleteWorkspaceFile(page, label);
    if (ok === true) deleted++;
    else if (ok === false) missing++;
    else failed++;
  }

  console.log('\n' + '='.repeat(60));
  console.log(`  ✅ Workspace cleanup: ${deleted} deleted, ${missing} not found, ${failed} failed`);
  console.log('='.repeat(60));

  await pause(1500);
  await browser.close();
})();
