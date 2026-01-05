import * as fs from "node:fs";
import * as path from "node:path";
import * as XLSX from "xlsx";

export type KeywordStat = {
  keyword: string;
  count: number;
  avgPlayers: number;
};

// --- CSV (no dependency) ---
function csvEscape(value: unknown): string {
  const s = String(value ?? "");
  // If contains comma, quote, newline => wrap in quotes and escape quotes
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

export function writeCsv(filePath: string, rows: KeywordStat[]) {
  const header = ["keyword", "count", "avgPlayers"];
  const lines = [
    header.join(","),
    ...rows.map((r) => [r.keyword, r.count, r.avgPlayers].map(csvEscape).join(",")),
  ];
  fs.writeFileSync(filePath, lines.join("\n"), "utf8");
}

// --- Excel / ODS via SheetJS ---
export function writeSpreadsheet(
  filePath: string,
  rows: KeywordStat[],
  sheetName = "keywords"
) {
  const wb = XLSX.utils.book_new();
  const ws = XLSX.utils.json_to_sheet(rows, {
    header: ["keyword", "count", "avgPlayers"],
  });

  // Optional: make columns a bit nicer
  ws["!cols"] = [
    { wch: 28 }, // keyword
    { wch: 10 }, // count
    { wch: 12 }, // avgPlayers
  ];

  XLSX.utils.book_append_sheet(wb, ws, sheetName);

  // SheetJS infers format from extension: .xlsx, .xls, .ods, etc.
  XLSX.writeFile(wb, filePath);
}

export function ensureDir(p: string) {
  fs.mkdirSync(p, { recursive: true });
}

export function withExt(basePathNoExt: string, ext: string) {
  return `${basePathNoExt}.${ext.replace(/^\./, "")}`;
}

export function normalizeFormat(fmt: string) {
  const f = fmt.trim().toLowerCase();
  // allow a few aliases
  if (f === "excel") return "xlsx";
  if (f === "ods" || f === "opendocument") return "ods";
  if (f === "csv") return "csv";
  if (f === "json") return "json";
  if (f === "xlsx") return "xlsx";
  // "obs" probably meant ods; map it just in case
  if (f === "obs") return "ods";
  return f;
}

export function writeOutputs(opts: {
  outBasePathNoExt: string; // e.g. reports/2026-01-05_top-earning_top100_keywords
  formats: string[];        // e.g. ["json","csv","xlsx","ods"]
  rows: KeywordStat[];
}) {
  const { outBasePathNoExt, formats, rows } = opts;

  const outDir = path.dirname(outBasePathNoExt);
  ensureDir(outDir);

  for (const rawFmt of formats) {
    const fmt = normalizeFormat(rawFmt);

    if (fmt === "json") {
      fs.writeFileSync(withExt(outBasePathNoExt, "json"), JSON.stringify(rows, null, 2), "utf8");
    } else if (fmt === "csv") {
      writeCsv(withExt(outBasePathNoExt, "csv"), rows);
    } else if (fmt === "xlsx") {
      writeSpreadsheet(withExt(outBasePathNoExt, "xlsx"), rows, "keywords");
    } else if (fmt === "ods") {
      writeSpreadsheet(withExt(outBasePathNoExt, "ods"), rows, "keywords");
    } else {
      throw new Error(`Unsupported export format: "${rawFmt}"`);
    }
  }
}