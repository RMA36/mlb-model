import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "MLB YRFI/NRFI Model",
  description: "v4 LightGBM two-model daily predictions and P&L tracker",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
