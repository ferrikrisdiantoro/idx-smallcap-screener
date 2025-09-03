// src/lib/saveCsv.ts

/**
 * Ubah array of records menjadi CSV string.
 * - Secara default memakai urutan kolom dari rows[0], bisa override lewat argumen cols.
 * - Escaping untuk tanda kutip, koma, newline, dan trim whitespace.
 */
export function toCSV(
  rows: Array<Record<string, unknown>>,
  cols?: string[]
): string {
  const columns =
    cols && cols.length > 0
      ? cols
      : rows.length > 0
      ? Object.keys(rows[0])
      : [];

  const escapeCell = (val: unknown): string => {
    if (val === null || val === undefined) return "";
    const s = String(val);
    // escape double-quote
    const escaped = s.replace(/"/g, '""');
    // wrap with quotes if contains comma, quote, newline, or leading/trailing space
    return /[",\n]/.test(s) || /^\s|\s$/.test(s) ? `"${escaped}"` : escaped;
  };

  const header = columns.join(",");
  const lines = rows.map((r) => columns.map((c) => escapeCell((r as any)[c])).join(","));
  return [header, ...lines].join("\n");
}

/**
 * Trigger download CSV di browser.
 */
export function saveAs(csvContent: string, filename: string) {
  const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
