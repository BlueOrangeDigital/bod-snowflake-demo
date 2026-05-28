import { test, expect } from '@playwright/test';

test('test', async ({ page }) => {
  await page.goto('https://cqa66797.us-east-1.snowflakecomputing.com/oauth/authorize?response_type=code&client_id=3kwdvnjpUzxU6sqlkOoknyZ30jLvtA%3D%3D&scope=refresh_token&state=%7B%22isSecondaryUser%22%3Afalse%2C%22csrf%22%3A%22e91f396f%22%2C%22url%22%3A%22https%3A%2F%2Fcqa66797.us-east-1.snowflakecomputing.com%22%2C%22windowId%22%3A%225800a7f1-cfdf-4880-b47e-d6eb09a571e6%22%2C%22classicUIUrl%22%3A%22https%3A%2F%2Fcqa66797.us-east-1.snowflakecomputing.com%22%2C%22browserUrl%22%3A%22https%3A%2F%2Fapp.snowflake.com%2Fjyofxaz%2Fcqa66797%2F%23%2Fworkspaces%2Fws%2FUSER%2524%2FPUBLIC%2FDEFAULT%2524%2FSample%2520Queries.sql%22%2C%22originator%22%3A%22started-by-cb100-2026-04-10T16%3A12%3A57.283630207Z%22%2C%22oauthNonce%22%3A%22zyMuwwQ8PIoliJes%22%7D&redirect_uri=https%3A%2F%2Fapps-api.c1.us-east-1.aws.app.snowflake.com%2Fcomplete-oauth%2Fsnowflake&code_challenge=LOJMuu8DSRi4vSTFfdz9PPGhrI2Pw3mbfVCcfVgN72Y&code_challenge_method=S256');
  await page.locator('.alignItems-blt-6s0dn4.display-blt-3nfvp2').first().click();
  await page.getByRole('textbox', { name: 'Username' }).click();
  await page.getByRole('button', { name: 'Show navigation' }).click();
  await page.getByRole('link', { name: 'Projects' }).click();
  await page.getByTestId('create-nav-button').click();
  await page.locator('[id="__js2570"] > .bg').click();
  await page.locator('iframe[title="Workspaces"]').contentFrame().getByTestId('workspace:/USER%2524.PUBLIC.DEFAULT%2524/untitled.ipynb::ipynb-editor').getByRole('button', { name: 'SQL' }).click();
  await page.locator('iframe[title="Workspaces"]').contentFrame().getByRole('textbox', { name: 'Code Editor' }).fill('select');
});