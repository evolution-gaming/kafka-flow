/**
 * Swizzled @theme/Footer/Layout.
 *
 * Restores the Docusaurus 1 footer layout: a faded logo to the LEFT of the
 * link columns, instead of D3's default of centering the logo below them.
 * (Upstream renders `{links}` then a centered `.footer__bottom` with the
 * logo + copyright; here the logo moves into a flex row beside the links.)
 */
import React from "react";
import clsx from "clsx";
import {ThemeClassNames} from "@docusaurus/theme-common";
import styles from "./styles.module.css";

export default function FooterLayout({style, links, logo, copyright}) {
  return (
    <footer
      className={clsx(ThemeClassNames.layout.footer.container, "footer", {
        "footer--dark": style === "dark",
      })}>
      <div className="container container-fluid">
        <div className={styles.footerTop}>
          {logo && <div className={styles.footerLogo}>{logo}</div>}
          {links && <div className={styles.footerLinks}>{links}</div>}
        </div>
        {copyright && (
          <div className="footer__bottom text--center">{copyright}</div>
        )}
      </div>
    </footer>
  );
}
