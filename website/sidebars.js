// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    {
      type: "category",
      label: "Kafka-Flow",
      collapsed: false,
      items: ["overview", "setup", "faq", "styleguide", "persistence"],
    },
    {
      type: "category",
      label: "Design notes",
      collapsed: false,
      items: ["kafka-single-writer-design", "cassandra-single-writer-design"],
    },
  ],
};

module.exports = sidebars;
