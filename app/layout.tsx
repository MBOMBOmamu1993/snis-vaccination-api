import "./globals.css";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Dashboard PEV de routine — RDC",
  description: "Tableau de bord de la vaccination de routine (PEV/OMS — RDC). Données issues du DHIS2.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="fr">
      <body>{children}</body>
    </html>
  );
}
