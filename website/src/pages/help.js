import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import Layout from "@theme/Layout";

const supportLinks = [
  {
    title: "Browse Docs",
    content: (
      <>
        Learn more using the{" "}
        <Link to="/docs/overview">documentation on this site.</Link>
      </>
    ),
  },
  {
    title: "Join the community",
    content: "Ask questions about the documentation and project.",
  },
  {
    title: "Stay up to date",
    content: "Find out what's new with this project.",
  },
];

export default function Help() {
  return (
    <Layout title="Help" description="Need help with Kafka Flow?">
      <main className="container margin-vert--lg">
        <h1>Need help?</h1>
        <p>This project is maintained by a dedicated group of people.</p>
        <div className="row">
          {supportLinks.map((link, idx) => (
            <div key={idx} className={clsx("col col--4 margin-vert--md")}>
              <h2>{link.title}</h2>
              <p>{link.content}</p>
            </div>
          ))}
        </div>
      </main>
    </Layout>
  );
}
