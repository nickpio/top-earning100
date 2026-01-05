import fs from "node:fs";
import path from "node:path";
import { analyzeKeywords } from "../nlp/keywordStats";

type ReportRow = {
  name: string;
  playing: number;
};

function latestReport(): string {
  const dir = path.resolve(process.cwd(), "reports");
  const files = fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    // ✅ only your raw top-earning reports; exclude derived outputs
    .filter((f) => f.includes("_top-earning_top") && !f.endsWith("_keywords.json"))
    .sort();

  if (files.length === 0) {
    throw new Error(`No raw top-earning report found in ${dir}`);
  }
  return path.join(dir, files[files.length - 1]);
}

async function main() {
  const reportPath = latestReport();
  const raw = fs.readFileSync(reportPath, "utf8");
  const rows = JSON.parse(raw) as ReportRow[];

  // Optional sanity check (helps catch schema mismatch immediately)
  if (!Array.isArray(rows) || rows.length === 0 || typeof (rows as any)[0]?.name !== "string") {
    throw new Error(
      `Report at ${reportPath} doesn't look like the expected shape (rows with {name, playing}).`
    );
  }

  const stats = analyzeKeywords(rows);

  const outPath = reportPath.replace(".json", "_keywords.json");
  fs.writeFileSync(outPath, JSON.stringify(stats, null, 2), "utf8");

  console.log(`Analyzed ${rows.length} titles -> ${stats.length} keywords`);
  console.log(`Wrote: ${outPath}`);
  console.log(stats.slice(0, 10));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});