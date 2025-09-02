"use client";
import { useEffect, useState } from "react";

export default function Clock() {
  const [now, setNow] = useState<string>(() => new Date().toLocaleTimeString("id-ID"));
  useEffect(() => {
    const t = setInterval(() => setNow(new Date().toLocaleTimeString("id-ID")), 1000);
    return () => clearInterval(t);
  }, []);
  return <>{now}</>;
}
