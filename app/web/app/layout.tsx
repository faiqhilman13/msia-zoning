import "maplibre-gl/dist/maplibre-gl.css";
import "./globals.css";

import type { Metadata } from "next";
import { Bricolage_Grotesque, Fraunces } from "next/font/google";

const displayFont = Bricolage_Grotesque({
  subsets: ["latin"],
  variable: "--font-display",
  weight: ["400", "500", "700", "800"]
});

const serifFont = Fraunces({
  subsets: ["latin"],
  variable: "--font-serif",
  weight: ["400", "600", "700"]
});

export const metadata: Metadata = {
  title: "MBJB Development Map",
  description:
    "Public-facing MBJB municipal development map built from GeoJB permit polygons."
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={`${displayFont.variable} ${serifFont.variable}`}>{children}</body>
    </html>
  );
}
