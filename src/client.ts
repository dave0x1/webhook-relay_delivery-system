// client.ts
import { EventSource } from 'eventsource';
import fs from 'node:fs';
import path from 'node:path';

const RELAY_URL = process.env.RELAY_URL || 'http://localhost:3000/events';
const LOG_FILE = path.resolve(process.cwd(), 'activity.log');

function appendToLog(line: string) {
  const timestamp = new Date().toISOString();
  fs.appendFileSync(LOG_FILE, `[${timestamp}] ${line}\n`, 'utf-8');
}

console.log(`Starting client daemon. Connecting to ${RELAY_URL}...`);
const es = new EventSource(RELAY_URL);

es.onopen = () => {
  console.log('Connected to webhook relay SSE stream.');
  appendToLog('SYSTEM: Connected to webhook relay SSE stream');
};

es.onerror = (err) => {
  console.error('SSE connection lost or error occurred. Auto-reconnecting...', err);
  appendToLog('SYSTEM: Connection dropped, waiting for reconnect...');
};

// Listen for GitHub event types or generic message events
es.onmessage = (event) => {
  handleEvent('message', event);
};

// If using named events like res.write('event: push\n')
const githubEvents = ['push', 'pull_request', 'issues', 'ping'];
for (const eventName of githubEvents) {
  es.addEventListener(eventName, (event: any) => {
    handleEvent(eventName, event);
  });
}

function handleEvent(type: string, event: MessageEvent) {
  try {
    const data = JSON.parse(event.data);
    const { deliveryId, eventType, payload } = data;

    let summary = `Event: ${eventType} | Delivery ID: ${deliveryId}`;

    if (eventType === 'push') {
      const repo = payload.repository?.full_name || 'unknown repo';
      const ref = payload.ref || 'unknown ref';
      const commits = payload.commits?.length || 0;
      summary += ` | ${repo} (${ref}) -> ${commits} new commit(s)`;
    } else if (eventType === 'ping') {
      summary += ` | GitHub ping received (Zen: ${payload.zen || 'N/A'})`;
    }

    console.log(`\n[EVENT RECEIVED] ${summary}`);
    appendToLog(summary);
  } catch (err) {
    console.error('Failed to parse SSE payload:', err);
    appendToLog(`ERROR: Malformed event data: ${event.data}`);
  }
}