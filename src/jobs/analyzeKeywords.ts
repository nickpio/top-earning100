import fs from "node:fs";
import path from "node:path";
import { analyzeKeywords } from "../nlp/keywordStats";
import { writeOutputs } from "../export/exporters";
import { sortKeywordStats } from "../export/sort";
import { suppressDominatedUnigrams } from "../nlp/suppress";

type ReportRow = {
  name: string;
  playing: number;
};

function getArg(flag: string): string | undefined {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return undefined;
  return process.argv[idx + 1];
}

function latestReport(): string {
  const dir = path.resolve(process.cwd(), "reports");
  const files = fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    // only raw reports, not derived
    .filter((f) => f.includes("_top-earning_top") && !f.includes("_keywords"))
    .sort();

  if (files.length === 0) throw new Error(`No raw top-earning report found in ${dir}`);
  return path.join(dir, files[files.length - 1]);
}

async function main() {
  const reportPath = getArg("--file") ?? latestReport();
  const formatsArg = getArg("--formats") ?? "json,csv,xlsx"; // default
  const formats = formatsArg.split(",").map((s) => s.trim()).filter(Boolean);

  const raw = fs.readFileSync(reportPath, "utf8");
  const rows = JSON.parse(raw) as ReportRow[];

  if (!Array.isArray(rows) || rows.length === 0 || typeof (rows as any)[0]?.name !== "string") {
    throw new Error(`Report at ${reportPath} isn't {name, playing}[] as expected.`);
  }

  const statsUnsorted = analyzeKeywords(rows, { minN: 1, maxN: 3, minCount: 2 });
  let stats = sortKeywordStats(statsUnsorted);

  // suppress component unigrams dominated by phrases
  stats = suppressDominatedUnigrams(stats, { threshold: 0.8, minPhraseCount: 3 });

  // re-sort after suppression (optional but nice)
  stats = sortKeywordStats(stats);

  // Base name: same as report but suffix "_keywords"
  const outBaseNoExt = reportPath.replace(/\.json$/i, "_keywords");

  const unigrams = stats.filter((s) => !s.keyword.includes(" "));
  const phrases = stats.filter((s) => s.keyword.includes(" "));

  writeOutputs({ outBasePathNoExt: outBaseNoExt + "_phrases", formats, rows: phrases });
  writeOutputs({ outBasePathNoExt: outBaseNoExt + "_unigrams", formats, rows: unigrams });

  console.log(`Analyzed ${rows.length} titles -> ${stats.length} keywords`);
  console.log(`Wrote formats [${formats.join(", ")}] to base: ${outBaseNoExt}.*`);
  console.log(stats.slice(0, 10));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});