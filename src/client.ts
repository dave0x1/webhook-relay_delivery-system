// client.ts
import { EventSource } from 'eventsource';
import fs from 'node:fs';
import path from 'node:path';

const RELAY_URL = process.env.RELAY_URL || 'http://localhost:3000/events?consumer=laptop-client';
// Target path for your local log (e.g., inside your Jekyll repository or personal notes)
const LOG_FILE = path.resolve(process.cwd(), 'activity.md');

function formatMarkdownEvent(eventType: string, payload: any): string | null {
  const timestamp = new Date().toLocaleTimeString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
  });
  const repo = payload.repository?.full_name || 'unknown-repo';

  switch (eventType) {
    case 'push': {
      const branch = (payload.ref || '').replace('refs/heads/', '');
      const commits = payload.commits || [];
      if (commits.length === 0) return null; // Skip branch deletion or empty pushes

      const commitLines = commits
        .map((c: any) => `  - [\`${c.id.substring(0, 7)}\`] ${c.message.split('\n')[0]}`)
        .join('\n');

      return `* **${timestamp}** — Pushed ${commits.length} commit(s) to \`${repo}\` (\`${branch}\`):\n${commitLines}\n`;
    }

    case 'pull_request': {
      const action = payload.action;
      const pr = payload.pull_request;
      const isMerged = action === 'closed' && pr.merged;
      const statusText = isMerged ? 'merged' : action;

      return `* **${timestamp}** — PR #${pr.number} ${statusText} on \`${repo}\`: [${pr.title}](${pr.html_url})\n`;
    }

    case 'issues': {
      const issue = payload.issue;
      return `* **${timestamp}** — Issue #${issue.number} ${payload.action} on \`${repo}\`: [${issue.title}](${issue.html_url})\n`;
    }

    case 'ping':
      return `* **${timestamp}** — Ping received from \`${repo}\`\n`;

    default:
      return `* **${timestamp}** — \`${eventType}\` event received from \`${repo}\`\n`;
  }
}

function appendToDailyLog(markdownEntry: string) {
  const dateHeading = `## ${new Date().toISOString().split('T')[0]}\n\n`;

  // Create file or write date header if file doesn't exist
  if (!fs.existsSync(LOG_FILE)) {
    fs.writeFileSync(LOG_FILE, `# Daily Activity Log\n\n${dateHeading}`, 'utf-8');
  }

  fs.appendFileSync(LOG_FILE, markdownEntry, 'utf-8');
}

const es = new EventSource(RELAY_URL);

es.onopen = () => console.log('Connected to webhook relay.');
es.onerror = (err) => console.error('Connection dropped, reconnecting...', err);

es.onmessage = (event) => {
  try {
    const { eventType, payload } = JSON.parse(event.data);
    const formatted = formatMarkdownEvent(eventType, payload);

    if (formatted) {
      appendToDailyLog(formatted);
      console.log(`Logged ${eventType} to ${LOG_FILE}`);
    }
  } catch (err) {
    console.error('Failed to parse event data:', err);
  }
};