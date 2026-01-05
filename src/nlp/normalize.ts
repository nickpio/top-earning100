import { STOPWORDS, SYNONYMS } from "./stopwords";

const BAN_PATTERNS: RegExp[] = [
  /\bupd\d*\b/gi,
  /\bupdate\d*\b/gi,
  /\bv\d+(\.\d+)*\b/gi,
  /\bbeta\b/gi,
  /\balpha\b/gi,
  /\btest(ing)?\b/gi,
];

export function normalizeText(text: unknown): string[] {
  if (typeof text !== "string") return [];
  if (text.trim().length === 0) return [];

  let t = text.toLowerCase();

  for (const rx of BAN_PATTERNS) {
    t = t.replace(rx, " ");
  }

  return t
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .map((w) => SYNONYMS[w] ?? w)
    .filter((w) => w.length >= 3 && !STOPWORDS.has(w));
}