import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./lib/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Bleu clair OMS (emblème World Health Organization)
        oms: {
          50: "#e6f3fb",
          100: "#cce7f7",
          200: "#99cfef",
          300: "#66b7e7",
          400: "#33a0df",
          500: "#0093d5",
          600: "#0078ae",
          700: "#005a82",
          800: "#003d57",
          900: "#001f2c",
        },
        // Bleu marine OFFICIEL OMS #00205c (capture intranet OneWHO) — entête / sidebar / bandeaux
        navy: {
          DEFAULT: "#00205c",
          400: "#1f54b8",
          500: "#15479e",
          600: "#0a3a86",
          700: "#00205c",
          800: "#001a45",
          900: "#00132f",
        },
        danger: { 50: "#fff1f1", 100: "#ffdede", 200: "#f8b4b4", 500: "#e23636", 600: "#c81e1e", 700: "#9b1616" },
        warn: { 50: "#fff8eb", 100: "#feecc5", 200: "#fbd88a", 500: "#f59e0b", 600: "#c87b04" },
        good: { 50: "#eafaf1", 100: "#d2f3e0", 200: "#a7e8c3", 500: "#1f9d57", 600: "#178a44" },
        surface: {
          0: "#ffffff",
          50: "#f8fafc",
          100: "#f1f5f9",
          200: "#e2e8f0",
          300: "#cbd5e1",
          700: "#334155",
          800: "#1e293b",
          900: "#0f172a",
        },
      },
      fontFamily: {
        sans: ["ui-sans-serif", "system-ui", "-apple-system", "Segoe UI", "Roboto", "Inter", "Helvetica", "Arial", "sans-serif"],
      },
      boxShadow: {
        card: "0 1px 2px 0 rgba(15, 23, 42, 0.04), 0 2px 8px -2px rgba(15, 23, 42, 0.08)",
      },
    },
  },
  plugins: [],
};

export default config;
