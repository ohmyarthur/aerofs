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
    details: Written in Rust with Tokio for non-blocking I/O that dramatically outperforms pure Python.
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
<div class="feature-item fa-fade-in" style="animation-delay: 0.1s;">
<h3>⚡ Zero Blocking</h3>
<p>Standard file I/O blocks the event loop. aerofs ensures your server keeps serving requests while reading from disk.</p>
</div>
<div class="feature-item fa-fade-in" style="animation-delay: 0.2s;">
<h3>🔌 Drop-in Replacement</h3>
<p>Use <code>aerofs.open()</code> just like <code>open()</code>. No complex learning curve or paradigm shifts required.</p>
</div>
<div class="feature-item fa-fade-in" style="animation-delay: 0.3s;">
<h3>🚀 Production Ready</h3>
<p>Battle-tested on Linux and macOS (M1/M2/Intel). Powering high-load async applications in production.</p>
</div>
</div>

<div class="code-demo-container fa-fade-in" style="animation-delay: 0.4s;">
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

<div class="stats-section fa-fade-in" style="animation-delay: 0.5s; margin-top: 4rem;">
<div class="stats-grid">
<div class="stat-item">
<span class="stat-number">v4.0.7</span>
<span class="stat-label">Latest Version</span>
</div>
<div class="stat-item">
<span class="stat-number">Rust</span>
<span class="stat-label">Powered By Tokio</span>
</div>
<div class="stat-item">
<span class="stat-number">Python 3.9+</span>
<span class="stat-label">Supported</span>
</div>
</div>
</div>
</div>

<style>
.stats-grid {
  display: flex;
  justify-content: center;
  gap: 3rem;
  flex-wrap: wrap;
  padding: 2rem 0;
}

.stat-item {
  text-align: center;
  padding: 1.5rem 2rem;
  border-radius: 16px;
  background: linear-gradient(135deg, 
    rgba(128, 128, 128, 0.06) 0%, 
    rgba(128, 128, 128, 0.02) 100%
  );
  border: 1px solid rgba(128, 128, 128, 0.1);
  min-width: 140px;
  transition: all 0.3s cubic-bezier(0.19, 1, 0.22, 1);
}

.stat-item:hover {
  transform: translateY(-4px);
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.08);
  border-color: rgba(224, 120, 87, 0.2);
}

.stat-number {
  display: block;
  font-size: 1.5rem;
  font-weight: 800;
  background: linear-gradient(135deg, var(--vp-c-brand-1), var(--vp-c-brand-2));
  -webkit-background-clip: text;
  background-clip: text;
  -webkit-text-fill-color: transparent;
  margin-bottom: 0.5rem;
}

.stat-label {
  font-size: 0.85rem;
  opacity: 0.65;
  font-weight: 500;
}

.code-window div[class*='language-'] {
  margin: 0 !important;
  border-radius: 0 !important;
  border: none !important;
  box-shadow: none !important;
}

.code-window div[class*='language-']:hover {
  transform: none !important;
  box-shadow: none !important;
}
</style>
