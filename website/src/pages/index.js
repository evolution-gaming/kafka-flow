import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';

import styles from './index.module.css';

// Mirrors the original Docusaurus 1 homepage: a splash (logo, title, tagline,
// Overview / Setup / Sources buttons) followed by the Ligthweight / Fast /
// Scalable feature columns.
const features = [
  {
    title: 'Ligthweight',
    image: 'img/undraw_floating_61u6.svg',
    content: 'Implementation consists of several classes',
  },
  {
    title: 'Fast',
    image: 'img/undraw_fast_loading_0lbh.svg',
    content: 'Process your events with minimal latency',
  },
  {
    title: 'Scalable',
    image: 'img/undraw_server_cluster_jwwq.svg',
    content: 'Uses Kafka consumer groups for infinite scaling',
  },
];

function HomeSplash() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <img
          className={styles.projectLogo}
          src={useBaseUrl('img/undraw_fishing_hoxa.svg')}
          alt="Project Logo"
        />
        <h1 className="hero__title">{siteConfig.title}</h1>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link className="button button--secondary button--lg" to="/docs/overview">
            Overview
          </Link>
          <Link className="button button--secondary button--lg" to="/docs/setup">
            Setup
          </Link>
          <Link
            className="button button--secondary button--lg"
            href="https://github.com/evolution-gaming/kafka-flow">
            Sources
          </Link>
        </div>
      </div>
    </header>
  );
}

function Feature({title, image, content}) {
  return (
    <div className={clsx('col col--4', styles.feature)}>
      <img className={styles.featureImage} src={useBaseUrl(image)} alt={title} />
      <h2>{title}</h2>
      <p>{content}</p>
    </div>
  );
}

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout title={siteConfig.title} description={siteConfig.tagline}>
      <HomeSplash />
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
    </Layout>
  );
}
