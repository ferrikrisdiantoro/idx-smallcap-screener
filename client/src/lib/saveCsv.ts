// src/lib/saveCsv.ts

// Baris-baris data bertipe "record" sederhana:
export type Row = Record<string, unknown>;

/**
 * Ubah array of records menjadi CSV string.
 * - Urutan kolom default dari rows[0], bisa override via argumen cols.
 * - Escaping untuk tanda kutip, koma, newline, dan spasi di tepi.
 */
export function toCSV<T extends Row>(
  rows: T[],
  cols?: (keyof T)[]
): string {
  const columns: (keyof T)[] =
    cols && cols.length > 0
      ? cols
      : rows.length > 0
      ? (Object.keys(rows[0]) as (keyof T)[])
      : [];

  const escapeCell = (val: unknown): string => {
    if (val === null || val === undefined) return "";
    const s = String(val);
    const escaped = s.replace(/"/g, '""');
    return /[",\n]/.test(s) || /^\s|\s$/.test(s) ? `"${escaped}"` : escaped;
  };

  // header
  const header = columns.map((c) => String(c)).join(",");

  // rows
  const lines = rows.map((r) =>
    columns.map((c) => escapeCell(r[c])).join(",")
  );

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
