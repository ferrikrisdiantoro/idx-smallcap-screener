"use client";
export const SignalBadge = ({ signal }: { signal: "BUY" | "HOLD" }) => {
  return (
    <span className={signal === "BUY" ? "badge badge-green" : "badge badge-red"}>
      {signal === "BUY" ? "BUY / UP" : "HOLD / NO-UP"}
    </span>
  );
};
