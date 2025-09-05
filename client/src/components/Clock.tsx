"use client";
import { useEffect, useState } from "react";

/**
 * Clock yang aman dari hydration mismatch:
 * - Tidak merender waktu saat SSR (render placeholder kosong).
 * - Setelah mounted di client, baru mulai menampilkan & update setiap 1 detik.
 * - suppressHydrationWarning mencegah React menganggap teks awal sebagai mismatch.
 */
export default function Clock() {
  const [mounted, setMounted] = useState(false);
  const [now, setNow] = useState<string>("");

  useEffect(() => {
    setMounted(true);

    const tick = () => setNow(new Date().toLocaleTimeString("id-ID"));
    tick(); // set sekali saat mount
    const t = setInterval(tick, 1000);
    return () => clearInterval(t);
  }, []);

  // Saat SSR / sebelum mounted → render placeholder agar HTML awal stabil
  if (!mounted) {
    return <span suppressHydrationWarning>&nbsp;</span>;
  }

  return <span suppressHydrationWarning>{now}</span>;
}
