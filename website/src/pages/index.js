import React from "react";
import clsx from "clsx";
import Head from "@docusaurus/Head";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Layout from "@theme/Layout";

import styles from "./index.module.css";

// Mirrors the original Docusaurus 1 homepage: a splash (logo, title, tagline,
// Overview / Setup / Sources buttons) followed by the Ligthweight / Fast /
// Scalable feature columns.
const features = [
  {
    title: "Ligthweight",
    image: "img/undraw_floating_61u6.svg",
    content: "Implementation consists of several classes",
  },
  {
    title: "Fast",
    image: "img/undraw_fast_loading_0lbh.svg",
    content: "Process your events with minimal latency",
  },
  {
    title: "Scalable",
    image: "img/undraw_server_cluster_jwwq.svg",
    content: "Uses Kafka consumer groups for infinite scaling",
  },
];

function HomeSplash() {
  const {siteConfig} = useDocusaurusContext();
  const btn = clsx("button button--outline button--primary button--lg", styles.heroButton);
  return (
    <header className={styles.homeContainer}>
      <div className="container">
        <img
          className={styles.projectLogo}
          src={useBaseUrl("img/undraw_fishing_hoxa.svg")}
          alt="Project Logo"
        />
        <h1 className={styles.projectTitle}>{siteConfig.title}</h1>
        <p className={styles.projectTagline}>{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link className={btn} to="/docs/overview">Overview</Link>
          <Link className={btn} to="/docs/setup">Setup</Link>
          <Link className={btn} href="https://github.com/evolution-gaming/kafka-flow">
            Sources
          </Link>
        </div>
      </div>
    </header>
  );
}

function Feature({title, image, content}) {
  return (
    <div className={clsx("col col--4", styles.feature)}>
      <img className={styles.featureImage} src={useBaseUrl(image)} alt={title} />
      <h2>{title}</h2>
      <p>{content}</p>
    </div>
  );
}

function HomepageFeatures() {
  return (
    <main>
      <section className={styles.features}>
        <div className="container">
          <div className="row">
            {features.map((props, idx) => (
              <Feature key={idx} {...props} />
            ))}
          </div>
        </div>
      </section>
    </main>
  );
}

export default function Home() {
  // Property access instead of `const {siteConfig} = ...` destructuring: a
  // Codacy parser mis-reads the `{siteConfig}` pattern here as a lone block.
  const siteConfig = useDocusaurusContext().siteConfig;
  // Match the old site's homepage tab title: "Kafka Flow · <tagline>".
  const tabTitle = siteConfig.title + " · " + siteConfig.tagline;
  return (
    <Layout description={siteConfig.tagline}>
      <Head>
        <title>{tabTitle}</title>
      </Head>
      <HomeSplash />
      <HomepageFeatures />
    </Layout>
  );
}
