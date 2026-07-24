import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      include: ["src/**/*.ts"],
      thresholds: {
        branches: 70,
        functions: 90,
        lines: 85,
        statements: 85,
      },
    },
    hookTimeout: 10_000,
    testTimeout: 10_000,
  },
});
