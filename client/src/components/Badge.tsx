"use client";
import React from "react";

export const Badge: React.FC<{ tone?: "green" | "amber" | "red"; text: string }> = ({ tone = "green", text }) => {
  const cls =
    tone === "green" ? "badge badge-green" :
    tone === "amber" ? "badge badge-amber" :
    "badge badge-red";
  return <span className={cls}>{text}</span>;
};
