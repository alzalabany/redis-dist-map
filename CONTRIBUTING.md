# Contributing

Thanks for helping make `redis-dist-map` better.

## Local setup

You need Node.js 18+, npm, and `redis-server` on your path.

```bash
git clone https://github.com/alzalabany/redis-dist-map.git
cd redis-dist-map
npm install
npm test
```

Before opening a pull request, run:

```bash
npm run check
npm run coverage
npm run build
npm pack --dry-run
```

Keep changes focused, add tests for behavior changes, and update the README or
changelog when the public API changes.
