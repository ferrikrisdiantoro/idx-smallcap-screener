"use client";
import React, { useMemo, useState } from "react";
import { toCSV } from "@/utils/format";

type Props = {
  rows: Array<Record<string, unknown>>;
  caption?: React.ReactNode;
  searchable?: boolean;
  sortable?: boolean;
  exportable?: boolean;
  dense?: boolean;
};

const DataTable: React.FC<Props> = ({ rows, caption, searchable, sortable, exportable, dense }) => {
  const [query, setQuery] = useState("");
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortAsc, setSortAsc] = useState(true);

  if (!rows || rows.length === 0) return <div className="muted text-sm">Tidak ada data.</div>;

  const cols = Object.keys(rows[0]);

  const filtered = useMemo(() => {
    if (!query.trim()) return rows;
    const q = query.toLowerCase();
    return rows.filter(r => cols.some(c => String(r[c] ?? "").toLowerCase().includes(q)));
  }, [rows, query, cols]);

  const sorted = useMemo(() => {
    if (!sortable || !sortKey) return filtered;
    const copy = [...filtered];
    copy.sort((a, b) => {
      const av = a[sortKey]; const bv = b[sortKey];
      const na = Number(av); const nb = Number(bv);
      const bothNum = !Number.isNaN(na) && !Number.isNaN(nb);
      if (bothNum) return sortAsc ? na - nb : nb - na;
      return sortAsc ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
    });
    return copy;
  }, [filtered, sortKey, sortAsc, sortable]);

  const onSort = (key: string) => {
    if (!sortable) return;
    if (sortKey === key) setSortAsc(!sortAsc);
    else { setSortKey(key); setSortAsc(true); }
  };

  return (
    <div className="flex flex-col gap-3">
      {/* head tools */}
      {(caption || searchable || exportable) && (
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          {caption}
          <div className="flex items-center gap-2">
            {searchable && (
              <input
                className="h-9 rounded-lg border border-slate-700 bg-slate-900 px-3 text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500"
                placeholder="Search…"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
              />
            )}
            {exportable && (
              <button
                className="h-9 rounded-lg border border-slate-700 bg-slate-900 px-3 text-sm hover:bg-slate-800"
                onClick={() => {
                  const csv = toCSV(sorted);
                  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
                  const url = URL.createObjectURL(blob);
                  const a = document.createElement("a");
                  a.href = url; a.download = "table.csv"; a.click();
                  URL.revokeObjectURL(url);
                }}
              >
                Export CSV
              </button>
            )}
          </div>
        </div>
      )}

      <div className="table-wrap">
        <table className="table">
          <thead>
            <tr>
              {cols.map((c) => (
                <th
                  key={c}
                  className="th"
                  onClick={() => onSort(c)}
                  style={{ cursor: sortable ? "pointer" as const : "default" }}
                  title={sortable ? "Click to sort" : ""}
                >
                  <div className="flex items-center gap-2">
                    <span className="capitalize">{c.replaceAll("_", " ")}</span>
                    {sortable && sortKey === c && <span className="text-xs muted">{sortAsc ? "▲" : "▼"}</span>}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.map((r, i) => (
              <tr key={i} className={i % 2 ? "bg-slate-900/40" : ""}>
                {cols.map((c) => (
                  <td
                    key={c}
                    className={`td ${/^(close|volume|ret_|vol_|value|ratio|net|num|market_cap)/i.test(c) ? "num" : ""} ${dense ? "py-1.5" : ""}`}
                    title={String(r[c] ?? "")}
                  >
                    {String(r[c] ?? "")}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default DataTable;
