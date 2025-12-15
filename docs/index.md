---
layout: home

hero:
  name: "AEROFS"
  text: "High-Performance Async I/O"
  tagline: "Powered by Rust, Built for Python asyncio."
  actions:
    - theme: brand
      text: Get Started
      link: /guide/getting-started
    - theme: alt
      text: View API Reference
      link: /api/core

features:
  - title: Blazing Fast
    icon:
      svg: '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M15.59 14.37a6 6 0 01-5.84 7.38v-4.8m5.84-2.58a14.98 14.98 0 006.16-12.12A14.98 14.98 0 009.631 8.41m5.96 5.96a14.926 14.926 0 01-5.841 2.58m-.119-8.54a6 6 0 00-7.381 5.84h4.8m2.581-5.84a14.927 14.927 0 00-2.58 5.84m2.699 2.7c-.103.021-.207.041-.311.06a15.09 15.09 0 01-2.448-2.448 14.9 14.9 0 01.06-.312m-2.24 2.39a4.493 4.493 0 00-1.757 4.306 4.493 4.493 0 004.306-1.758M16.5 9a1.5 1.5 0 11-3 0 1.5 1.5 0 013 0z" /></svg>'
    details: Written in Rust with Tokio for non-blocking I/O that outperforms pure Python implementations.
  - title: True Async
    icon:
      svg: '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M3.75 13.5l10.5-11.25L12 10.5h8.25L9.75 21.75 12 13.5H3.75z" /></svg>'
    details: Offloads file operations to a thread pool, preventing event loop blocking in asyncio apps.
  - title: Pythonic API
    icon:
      svg: '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M17.25 6.75L22.5 12l-5.25 5.25m-10.5 0L1.5 12l5.25-5.25m7.5-3l-4.5 18" /></svg>'
    details: Drop-in replacement for standard open(), os, and tempfile modules.
  - title: Type Safe
    icon:
      svg: '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12c0 1.268-.63 2.39-1.593 3.068a3.745 3.745 0 01-1.043 3.296 3.745 3.745 0 01-3.296 1.043A3.745 3.745 0 0112 21c-1.268 0-2.39-.63-3.068-1.593a3.746 3.746 0 01-3.296-1.043 3.745 3.745 0 01-1.043-3.296A3.745 3.745 0 013 12c0-1.268.63-2.39 1.593-3.068a3.745 3.745 0 011.043-3.296 3.746 3.746 0 013.296-1.043A3.746 3.746 0 0112 3c1.268 0 2.39.63 3.068 1.593a3.746 3.746 0 013.296 1.043 3.746 3.746 0 011.043 3.296A3.745 3.745 0 0121 12z" /></svg>'
    details: Fully typed and compatible with modern Python tooling and static analysis.

---

<div class="landing-content">
<div class="feature-grid">
<div class="feature-item">
<h3>Zero Blocking</h3>
<p>Standard file I/O blocks the event loop. aerofs ensures your server keeps serving requests while reading from disk.</p>
</div>
<div class="feature-item">
<h3>Drop-in Replacement</h3>
<p>Use <code>aerofs.open()</code> just like <code>open()</code>. No complex learning curve.</p>
</div>
<div class="feature-item">
<h3>Production Ready</h3>
<p>Tested on Linux and macOS (M1/M2/Intel). Powering high-load async applications.</p>
</div>
</div>

<div class="code-demo-container fa-fade-in">
<h3>Simple, Elegant, Fast.</h3>
<div class="code-window">
<div class="window-header">
<span class="dot red"></span>
<span class="dot yellow"></span>
<span class="dot green"></span>
</div>

```python
import aerofs
import asyncio

async def process_logs():
    # Opened asynchronously, strictly non-blocking
    async with aerofs.open('/var/log/syslog', 'r') as f:
        async for line in f:
            if "ERROR" in line:
                await send_alert(line)

asyncio.run(process_logs())
```

</div>
</div>
</div>

<style>
.landing-content {
  text-align: center;
  margin-top: 4rem;
  padding: 0 1.5rem;
}

.feature-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 2rem;
  margin-bottom: 4rem;
}

.feature-item h3 {
  font-weight: 600;
  margin-bottom: 0.5rem;
  background: -webkit-linear-gradient(120deg, var(--vp-c-brand-1), var(--vp-c-brand-2));
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}

.code-demo-container {
  max-width: 800px;
  margin: 0 auto;
}

.code-window {
  background: #1e1e1e;
  border-radius: 8px;
  box-shadow: 0 20px 50px rgba(0,0,0,0.3);
  overflow: hidden;
  text-align: left;
  font-family: var(--vp-font-family-mono);
}

.window-header {
  background: #2d2d2d;
  padding: 12px 16px;
  display: flex;
  gap: 8px;
}

.dot {
  width: 12px;
  height: 12px;
  border-radius: 50%;
}
.red { background: #ff5f56; }
.yellow { background: #ffbd2e; }
.green { background: #27c93f; }

.code-window div[class*='language-'] {
  margin: 0 !important;
  border-radius: 0 !important;
}

.fa-fade-in {
  animation: fadeIn 1s ease-out;
}

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(20px); }
  to { opacity: 1; transform: translateY(0); }
}
</style>
