#!/usr/bin/env node
/**
 * Voice sampler for picking a narrator voice for run_demo.js.
 *
 * Iterates a curated set of macOS `say` voices spanning gender, region, pitch,
 * and timbre. Speaks the same test sentence with each and prints the voice's
 * name + locale + a short description, plus the exact `say` flag to plug into
 * run_demo.js to use it.
 *
 * Usage:
 *   node demo/test_voices.js                  # default rate 155 wpm
 *   node demo/test_voices.js --rate 180       # custom rate
 *   node demo/test_voices.js --filter karen   # only voices whose name matches
 */

const { execSync, exec } = require('child_process');

const TEST_SENTENCE =
  "Welcome! Today I'm demonstrating Snowflake AI and Cortex " +
  "with a real-world use case: analyzing financial markets.";

// Curated voices to compare. Each entry: { name, locale, gender, notes }.
// The script will skip any not installed on this machine.
const VOICES = [
  // Realistic English voices, varied region and gender
  { name: 'Samantha', locale: 'en_US', gender: 'female', notes: 'macOS default — neutral US female, professional' },
  { name: 'Alex',     locale: 'en_US', gender: 'male',   notes: 'Premium US male — natural, narrator-quality (may need download)' },
  { name: 'Daniel',   locale: 'en_GB', gender: 'male',   notes: 'British male — calm and authoritative' },
  { name: 'Karen',    locale: 'en_AU', gender: 'female', notes: 'Australian female — warm, slightly higher pitch' },
  { name: 'Moira',    locale: 'en_IE', gender: 'female', notes: 'Irish female — lilting cadence' },
  { name: 'Tessa',    locale: 'en_ZA', gender: 'female', notes: 'South African female — crisp diction' },
  { name: 'Rishi',    locale: 'en_IN', gender: 'male',   notes: 'Indian male — formal, lower register' },
  { name: 'Aman',     locale: 'en_IN', gender: 'male',   notes: 'Indian male — younger, brisker' },
  { name: 'Tara',     locale: 'en_IN', gender: 'female', notes: 'Indian female — clear, mid-pitch' },

  // Distinct US timbre options
  { name: 'Fred',     locale: 'en_US', gender: 'male',   notes: 'Classic deep US male — low pitch, robotic edge' },
  { name: 'Ralph',    locale: 'en_US', gender: 'male',   notes: 'Older US male — gravelly, deliberate' },
  { name: 'Kathy',    locale: 'en_US', gender: 'female', notes: 'US female — light timbre, friendly' },
  { name: 'Albert',   locale: 'en_US', gender: 'male',   notes: 'Robotic-classic male — vintage Mac vibe' },

  // Novelty / character voices (probably not for a demo, but useful for comparison)
  { name: 'Whisper',  locale: 'en_US', gender: 'n/a',    notes: 'Whispered — too quiet for demos' },
  { name: 'Bells',    locale: 'en_US', gender: 'n/a',    notes: 'Musical bells — pure novelty' },
];

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = { rate: 155, filter: null };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--rate') opts.rate = parseInt(args[++i], 10) || 155;
    else if (args[i] === '--filter') opts.filter = (args[++i] || '').toLowerCase();
  }
  return opts;
}

function getInstalledVoiceNames() {
  // `say -v ?` outputs lines like: "Samantha   en_US   # Hello! ..."
  // The voice name can contain spaces and parenthetical suffixes, so capture
  // everything up to the locale token (e.g. en_US, en_GB).
  const out = execSync('say -v "?"', { encoding: 'utf8' });
  const names = new Set();
  for (const line of out.split('\n')) {
    const m = line.match(/^(.+?)\s+([a-z]{2}_[A-Z]{2})\s+#/);
    if (m) names.add(m[1].trim());
  }
  return names;
}

function speak(text, voice, rate) {
  return new Promise((resolve) => {
    const safe = JSON.stringify(text);
    exec(`say -v ${JSON.stringify(voice)} -r ${rate} ${safe}`, (err) => resolve(err));
  });
}

function pause(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

(async () => {
  const { rate, filter } = parseArgs();
  const installed = getInstalledVoiceNames();

  const candidates = VOICES.filter((v) => {
    if (!installed.has(v.name)) return false;
    if (filter && !v.name.toLowerCase().includes(filter)) return false;
    return true;
  });

  if (candidates.length === 0) {
    console.error('No matching voices installed. Try: say -v "?" to see what is available.');
    process.exit(1);
  }

  console.log('='.repeat(72));
  console.log(`  Voice sampler — speaking with ${candidates.length} voices @ ${rate} wpm`);
  console.log('='.repeat(72));
  console.log(`\nTest sentence:\n  "${TEST_SENTENCE}"\n`);

  for (const v of candidates) {
    console.log('─'.repeat(72));
    console.log(`  🎙  ${v.name}  (${v.locale}, ${v.gender})`);
    console.log(`     ${v.notes}`);
    console.log(`     Use in run_demo.js:  say -v "${v.name}" -r ${rate} "..."`);
    console.log('');
    await speak(TEST_SENTENCE, v.name, rate);
    await pause(500);
  }

  console.log('='.repeat(72));
  console.log('  Done. To use a voice in run_demo.js, change the speak() function:');
  console.log('    Line ~89:  exec(`say -r 155 ${arg}`)   →');
  console.log('               exec(`say -v "Samantha" -r 155 ${arg}`)');
  console.log('  (replace "Samantha" with the voice name you preferred above)');
  console.log('='.repeat(72));
})();
