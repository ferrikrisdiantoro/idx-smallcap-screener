import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Hasil Backtesting Sinyal Bandar",
  description: "Menampilkan sinyal Beli & Jual yang terdeteksi oleh skrip Python.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="id">
      <body>
        <div className="container mx-auto max-w-6xl py-6">{children}</div>
      </body>
    </html>
  );
}
